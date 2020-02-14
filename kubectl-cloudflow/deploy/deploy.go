package deploy

import (
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-akka/configuration"
	"github.com/lightbend/cloudflow/kubectl-cloudflow/docker"
	"github.com/lightbend/cloudflow/kubectl-cloudflow/domain"
	"github.com/lightbend/cloudflow/kubectl-cloudflow/util"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth" // Import additional authentication methods
)

// GetCloudflowApplicationDescriptorFromDockerImage pulls a image and extracts the Cloudflow Application descriptor from a docker label
func GetCloudflowApplicationDescriptorFromDockerImage(dockerRegistryURL string, dockerRepository string, dockerImagePath string) (domain.CloudflowApplicationSpec, docker.PulledImage) {

	apiversion, apierr := exec.Command("docker", "version", "--format", "'{{.Server.APIVersion}}'").Output()
	if apierr != nil {
		util.LogAndExit("Could not get docker API version, is the docker daemon running? API error: %s", apierr.Error())
	}

	trimmedapiversion := strings.Trim(string(apiversion), "\t \n\r'")
	client, error := docker.GetClient(trimmedapiversion)
	if error != nil {
		client, error = docker.GetClient("1.39")
		if error != nil {
			fmt.Printf("No compatible version of the Docker server API found, tried version %s and 1.39", trimmedapiversion)
			panic(error)
		}
	}

	pulledImage, pullError := docker.PullImage(client, dockerImagePath)
	if pullError != nil {
		util.LogAndExit("Failed to pull image %s: %s", dockerImagePath, pullError.Error())
	}

	applicationDescriptorImageDigest := docker.GetCloudflowApplicationDescriptor(client, dockerImagePath)

	var spec domain.CloudflowApplicationSpec
	marshalError := json.Unmarshal([]byte(applicationDescriptorImageDigest.AppDescriptor), &spec)
	if marshalError != nil {
		fmt.Print("\n\nAn unexpected error has occurred, please contact support and include the information below.\n\n")
		panic(marshalError)
	}

	if spec.Version != domain.SupportedApplicationDescriptorVersion {
		// If the version is an int, compare them, otherwise provide a more general message.
		if version, err := strconv.Atoi(spec.Version); err == nil {
			if supportedVersion, err := strconv.Atoi(domain.SupportedApplicationDescriptorVersion); err == nil {
				if version < supportedVersion {
					if spec.LibraryVersion != "" {
						util.LogAndExit("Image %s, built with sbt-cloudflow version '%s', is incompatible and no longer supported. Please upgrade sbt-cloudflow and rebuild the image.", dockerImagePath, spec.LibraryVersion)
					} else {
						util.LogAndExit("Image %s is incompatible and no longer supported. Please upgrade sbt-cloudflow and rebuild the image.", dockerImagePath)
					}
				}
				if version > supportedVersion {
					if spec.LibraryVersion != "" {
						util.LogAndExit("Image %s, built with sbt-cloudflow version '%s', is incompatible and requires a newer version of the kubectl cloudflow plugin. Please upgrade and try again.", dockerImagePath, spec.LibraryVersion)
					} else {
						util.LogAndExit("Image %s is incompatible and requires a newer version of the kubectl cloudflow plugin. Please upgrade and try again.", dockerImagePath)
					}
				}
			}
		}

		util.LogAndExit("Image %s is incompatible and no longer supported. Please update sbt-cloudflow and rebuild the image.", dockerImagePath)
	}

	digest := applicationDescriptorImageDigest.ImageDigest

	var imageRef string
	if dockerRegistryURL == "" {
		imageRef = dockerRepository + "/" + digest
	} else {
		imageRef = dockerRegistryURL + "/" + dockerRepository + "/" + digest
	}

	// replace tagged images with digest based names
	for i := range spec.Deployments {
		spec.Deployments[i].Image = imageRef
	}
	return spec, *pulledImage
}

// GetCloudflowApplicationSpecFromDockerImages pulls a image and extracts the Cloudflow Application descriptor from a docker label
func GetCloudflowApplicationSpecFromDockerImages(imageRefs []ImageReference,
	blueprintConfig *configuration.Config) (domain.CloudflowApplicationSpec, []*docker.PulledImage) {

	apiversion, apierr := exec.Command("docker", "version", "--format", "'{{.Server.APIVersion}}'").Output()
	if apierr != nil {
		util.LogAndExit("Could not get docker API version, is the docker daemon running? API error: %s", apierr.Error())
	}

	trimmedapiversion := strings.Trim(string(apiversion), "\t \n\r'")
	client, error := docker.GetClient(trimmedapiversion)
	if error != nil {
		client, error = docker.GetClient("1.39")
		if error != nil {
			fmt.Printf("No compatible version of the Docker server API found, tried version %s and 1.39", trimmedapiversion)
			panic(error)
		}
	}

	// get all streamlet descriptors, image digests and pulled images in arrays
	var streamletDescriptors []domain.Descriptor
	var imageDigests []string
	var apiVersion string
	var pulledImages []*docker.PulledImage
	var deployImages = make(map[string]string)
	for _, imageRef := range imageRefs {
		streamletsDescriptorsDigestPair, version := docker.GetCloudflowStreamletDescriptorsForImage(client, imageRef.FullURI)
		if version != domain.SupportedApplicationDescriptorVersion {
			util.LogAndExit("Image %s is incompatible and no longer supported. Please update sbt-cloudflow and rebuild the image.", imageRef.FullURI)
		}
		streamletDescriptors = append(streamletsDescriptorsDigestPair.StreamletDescriptors, streamletDescriptors...)
		imageDigests = append(imageDigests, streamletsDescriptorsDigestPair.ImageDigest)
		apiVersion = version

		pulledImage, pullError := docker.PullImage(client, imageRef.FullURI)
		if pullError != nil {
			util.LogAndExit("Failed to pull image %s: %s", imageRef.FullURI, pullError.Error())
		}
		pulledImages = append(pulledImages, pulledImage)

		// this format has to be used in Deployment: full-uri@sha
		deployImage := fmt.Sprintf("%s@%s", strings.Split(pulledImage.ImageName, ":")[0], strings.Split(streamletsDescriptorsDigestPair.ImageDigest, "@")[1])
		deployImages[imageRef.FullURI] = deployImage
	}

	// load the blueprint
	bp := makeBlueprint(blueprintConfig, imageDigests)
	fmt.Println(bp)

	// Spec.Streamlets & Spec.Deployments
	var streamlets []domain.Streamlet
	var deployments []domain.Deployment
	var index = 0
	for _, streamletDescriptor := range streamletDescriptors {
		streamletIDs := bp.getStreamletIDsFromClassName(streamletDescriptor.ClassName)
		if len(streamletIDs) == 0 {
			continue
		}

		for _, streamletID := range streamletIDs {
			streamlet := domain.Streamlet{Descriptor: streamletDescriptor, Name: streamletID}
			streamlets = append(streamlets, streamlet)
			deployments = append(deployments, makeDeployment(bp, streamletID, streamlet, deployImages, index))
			index++
		}
	}

	// Spec.Connections
	var conns []domain.Connection
	for inlet, outlets := range bp.connections {
		for _, outlet := range outlets {
			// need to switch inlet and outlet as we have them reversed in the blueprint map of connections
			conns = append(conns,
				domain.Connection{InletName: outlet.port,
					InletStreamletName: outlet.streamletID, OutletName: inlet.port, OutletStreamletName: inlet.streamletID})
		}
	}

	var spec domain.CloudflowApplicationSpec
	spec.AgentPaths = map[string]string{
		"prometheus": "/prometheus/jmx_prometheus_javaagent-0.11.0.jar",
	}
	spec.AppID = bp.name
	spec.AppVersion = "1.3.1-SNAPSHOT"
	spec.Version = apiVersion
	spec.LibraryVersion = domain.LibraryVersion
	spec.Connections = conns
	spec.Streamlets = streamlets
	spec.Deployments = deployments
	return spec, pulledImages
}

func makeDeployment(bp blueprint, streamletID string, streamlet domain.Streamlet, deployImages map[string]string, index int) domain.Deployment {
	var deployment domain.Deployment
	deployment.StreamletName = streamletID
	deployment.ClassName = streamlet.Descriptor.ClassName
	image, err := bp.getImageFromStreamletID(streamletID)
	if err != nil {
		util.LogAndExit(err.Error())
	}
	deployment.Image = deployImages[image]
	deployment.PortMappings = getAllPortMappings(bp.name, streamletID, streamlet.Descriptor, bp)
	deployment.VolumeMounts = streamlet.Descriptor.VolumeMounts
	deployment.Runtime = streamlet.Descriptor.Runtime
	deployment.SecretName = transformToDNS1123SubDomain(streamlet.Name)
	deployment.Name = fmt.Sprintf("%s.%s", bp.name, streamletID)
	deployment.Replicas = 1

	config, endpoint, err := getServerConfigAndEndpoint(bp.name, streamlet, index)
	if err != nil {
		util.LogAndExit(err.Error())
	}
	if config != nil {
		deployment.Config = config
	}
	if endpoint != (domain.Endpoint{}) {
		deployment.Endpoint = &endpoint
	}
	return deployment
}

// type Deployment struct {
// 	ClassName     string                  `json:"class_name"`
// 	Config        json.RawMessage         `json:"config"`
// 	Image         string                  `json:"image"`
// 	Name          string                  `json:"name"`
// 	PortMappings  map[string]PortMapping  `json:"port_mappings"`
// 	VolumeMounts  []VolumeMountDescriptor `json:"volume_mounts"`
// 	Runtime       string                  `json:"runtime"`
// 	StreamletName string                  `json:"streamlet_name"`
// 	SecretName    string                  `json:"secret_name"`
// 	Endpoint      *Endpoint               `json:"endpoint,omitempty"`
// 	Replicas      int                     `json:"replicas,omitempty"`
// }

// Removes from the leading and trailing positions the specified characters.
func trim(name string) string {
	return strings.TrimSuffix(strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(name, "."), "-"), "."), "-")
}

// Make a name compatible with DNS 1123 Subdomain
// Limit the resulting name to 253 characters
func transformToDNS1123SubDomain(name string) string {
	splits := strings.Split(name, ".")
	var normalized []string
	for _, split := range splits {
		normalized = append(normalized, trim(normalize(split)))
	}
	joined := strings.Join(normalized[:], ".")
	if len(joined) > subDomainMaxLength {
		return strings.TrimSuffix(joined[0:subDomainMaxLength], ".")
	}
	return strings.TrimSuffix(joined, ".")
}

func normalize(name string) string {
	t := transform.Chain(norm.NFKD)
	normalizedName, _, _ := transform.String(t, name)
	s := strings.ReplaceAll(strings.ReplaceAll(strings.ToLower(normalizedName), "_", "-"), ".", "-")
	var re = regexp.MustCompile(`[^-a-z0-9]`)
	return re.ReplaceAllString(s, "")
}

func getServerAttribute(descriptor domain.Descriptor) domain.Attribute {
	var attr domain.Attribute
	for _, attribute := range descriptor.Attributes {
		if attribute.AttributeName == "server" {
			attr = attribute
			return attribute
		}
	}
	return attr
}

// MinimumEndpointContainerPort indicates the minimum value of the end point port
const minimumEndpointContainerPort = 3000
const subDomainMaxLength = 253

type configRoot struct {
	Cloudflow internal `json:"cloudflow,omitempty"`
}
type internal struct {
	Internal server `json:"internal,omitempty"`
}
type server struct {
	Server containerPort `json:"server,omitempty"`
}
type containerPort struct {
	ContainerPort int `json:"container-port,omitempty"`
}

func getServerConfigAndEndpoint(appID string, streamlet domain.Streamlet, index int) (json.RawMessage, domain.Endpoint, error) {
	var endPoint domain.Endpoint
	containerPrt := minimumEndpointContainerPort + index
	attribute := getServerAttribute(streamlet.Descriptor)
	if attribute == (domain.Attribute{}) {
		x, _ := json.Marshal(containerPort{})
		return x, domain.Endpoint{}, nil
	}
	endPoint = domain.Endpoint{AppID: appID, Streamlet: streamlet.Name, ContainerPort: containerPrt}
	c := containerPort{ContainerPort: containerPrt}
	s := server{Server: c}
	i := internal{Internal: s}
	r := configRoot{Cloudflow: i}
	bytes, _ := json.Marshal(r)
	return bytes, endPoint, nil
}

func getServerConfigAndEndpoint1(appID string, streamlet domain.Streamlet, index int) (json.RawMessage, domain.Endpoint, error) {
	var endPoint domain.Endpoint
	containerPort := minimumEndpointContainerPort + index
	attribute := getServerAttribute(streamlet.Descriptor)
	if attribute == (domain.Attribute{}) {
		j, err := json.Marshal(configuration.Config{})
		if err != nil {
			util.LogAndExit(err.Error())
		}
		return json.RawMessage(j), domain.Endpoint{}, nil
	}
	endPoint = domain.Endpoint{AppID: appID, Streamlet: streamlet.Name, ContainerPort: containerPort}
	// j, err := json.Marshal(configuration.ParseString(fmt.Sprintf("{%s = %d}", attribute.ConfigPath, containerPort)))
	j, err := json.Marshal(configuration.ParseString(fmt.Sprintf(`{%s = %d}`, attribute.ConfigPath, containerPort)))
	if err != nil {
		util.LogAndExit(err.Error())
	}
	config := json.RawMessage(j)
	return config, endPoint, nil
}

func makeBlueprint(blueprintConfig *configuration.Config, imageDigests []string) blueprint {
	// get name
	name := blueprintConfig.GetString("blueprint.name", strings.Split(imageDigests[0], "@")[0])

	// get images
	imagesRoot := blueprintConfig.GetValue("blueprint.images").GetObject()
	imageIDs := imagesRoot.GetKeys()

	var imageRefs []imageRef
	for _, imageID := range imageIDs {
		imageRefs = append(imageRefs, imageRef{imageID: imageID, imageURI: imagesRoot.GetKey(imageID).String()})
	}

	// get streamlets
	streamletsRoot := blueprintConfig.GetValue("blueprint.streamlets").GetObject()
	streamletIDs := streamletsRoot.GetKeys()

	var streamletRefs []streamletRef
	for _, streamletID := range streamletIDs {
		streamlet := streamletsRoot.GetKey(streamletID).String()
		streamletSplit := strings.Split(streamlet, "/")
		streamletRefs = append(streamletRefs, streamletRef{imageID: streamletSplit[0], streamletID: streamletID, className: streamletSplit[1]})
	}

	// get connections
	connectionsRoot := blueprintConfig.GetValue("blueprint.connections").GetObject()
	aStreamletIDs := connectionsRoot.GetKeys()
	connectionMap := make(map[streamletEndpoint][]streamletEndpoint)
	for _, aStreamletID := range aStreamletIDs {
		aStreamletConnections := connectionsRoot.GetKey(aStreamletID).GetObject().Items()
		for aPort, bStreamletConnections := range aStreamletConnections {
			var bStreamletEndpoints []streamletEndpoint
			for _, bStreamletConnection := range bStreamletConnections.GetArray() {
				bStreamletConnectionSplit := strings.Split(bStreamletConnection.String(), ".")
				bStreamletEndpoints = append(bStreamletEndpoints,
					streamletEndpoint{streamletID: bStreamletConnectionSplit[0], port: bStreamletConnectionSplit[1]})
			}
			connectionMap[streamletEndpoint{streamletID: aStreamletID, port: aPort}] = bStreamletEndpoints
		}
	}

	return blueprint{name: name, images: imageRefs, streamlets: streamletRefs, connections: connectionMap}
}

// getStreamletIDsFromClassName fetches the streamlet refs from blueprint
// note there can be multiple refs for a single class e.g.
// streamlets {
//	cdr-generator1 = ings/carly.aggregator.CallRecordGeneratorIngress
//	cdr-generator2 = ings/carly.aggregator.CallRecordGeneratorIngress
//  ...
// }
func (b blueprint) getStreamletIDsFromClassName(className string) []string {
	var streamletIDs []string
	for _, streamletRef := range b.streamlets {
		if streamletRef.className == className {
			streamletIDs = append(streamletIDs, streamletRef.streamletID)
		}
	}
	return streamletIDs
}

func (b blueprint) getImageFromStreamletID(streamletID string) (string, error) {
	var imageID string
	for _, streamletRef := range b.streamlets {
		if streamletRef.streamletID == streamletID {
			imageID = streamletRef.imageID
			break
		}
	}
	for _, image := range b.images {
		if image.imageID == imageID {
			return image.imageURI, nil
		}
	}
	return "", fmt.Errorf("Image not found for streamlet %s", streamletID)
}

func getAllPortMappings(appID string, streamletName string, descriptor domain.Descriptor,
	blueprint blueprint) map[string]domain.PortMapping {
	allPortMappings := make(map[string]domain.PortMapping)

	for k, v := range getOutletPortMappings(appID, streamletName, descriptor) {
		allPortMappings[k] = v
	}
	for k, v := range getInletPortMappings(appID, streamletName, blueprint) {
		allPortMappings[k] = v
	}
	return allPortMappings
}

func getOutletPortMappings(appID string, streamletName string, descriptor domain.Descriptor) map[string]domain.PortMapping {
	portMappings := make(map[string]domain.PortMapping)
	for _, outlet := range descriptor.Outlets {
		portMappings[outlet.Name] = domain.PortMapping{AppID: appID, Streamlet: streamletName, Outlet: outlet.Name}
	}
	return portMappings
}

func getInletPortMappings(appID string, streamletName string, blueprint blueprint) map[string]domain.PortMapping {
	portMappings := make(map[string]domain.PortMapping)
	for outEndpoint, inEndpoints := range blueprint.connections {
		for _, inEndpoint := range inEndpoints {
			if inEndpoint.streamletID == streamletName {
				portMappings[inEndpoint.port] = domain.PortMapping{AppID: appID, Outlet: outEndpoint.port, Streamlet: outEndpoint.streamletID}
			}
		}
	}
	return portMappings
}

func splitOnFirstCharacter(str string, char byte) ([]string, error) {
	var arr []string
	if idx := strings.IndexByte(str, char); idx >= 0 {
		arr = append(arr, str[:idx])
		arr = append(arr, strings.Trim(str[idx+1:], "\""))
		return arr, nil
	}
	return arr, fmt.Errorf("The configuration parameters must be formated as space delimited '[streamlet-name[.[property]=[value]' pairs")
}

// SplitConfigurationParameters maps string representations of a key/value pair into a map
func SplitConfigurationParameters(configurationParameters []string) map[string]string {
	configurationKeyValues := make(map[string]string)

	for _, v := range configurationParameters {
		keyValueArray, err := splitOnFirstCharacter(v, '=')
		if err != nil {
			util.LogAndExit(err.Error())
		} else {
			configurationKeyValues[keyValueArray[0]] = keyValueArray[1]

		}
	}
	return configurationKeyValues
}

// AppendExistingValuesNotConfigured adds values for those keys that are not entered by the user, which do already exist in secrets
func AppendExistingValuesNotConfigured(client *kubernetes.Clientset, spec domain.CloudflowApplicationSpec, configurationKeyValues map[string]string) map[string]string {
	existingConfigurationKeyValues := make(map[string]string)
	for _, deployment := range spec.Deployments {
		if secret, err := client.CoreV1().Secrets(spec.AppID).Get(deployment.SecretName, metav1.GetOptions{}); err == nil {
			for _, configValue := range secret.Data {
				lines := strings.Split(string(configValue), "\r\n")
				for _, line := range lines {
					cleaned := strings.TrimPrefix(strings.TrimSpace(line), "cloudflow.streamlets.")
					if len(cleaned) != 0 {
						keyValueArray, err := splitOnFirstCharacter(cleaned, '=')
						if err != nil {
							util.LogAndExit("Configuration for streamlet %s in secret %s is corrupted. %s", deployment.StreamletName, secret.Name, err.Error())
						} else {
							existingConfigurationKeyValues[keyValueArray[0]] = keyValueArray[1]
						}
					}
				}
			}
		}
	}

	for _, streamlet := range spec.Streamlets {
		for _, descriptor := range streamlet.Descriptor.ConfigParameters {
			fqKey := prefixWithStreamletName(streamlet.Name, descriptor.Key)
			if _, ok := configurationKeyValues[fqKey]; !ok {
				if _, ok := existingConfigurationKeyValues[fqKey]; ok {
					fmt.Printf("Existing value will be used for configuration parameter '%s'\n", fqKey)
					configurationKeyValues[fqKey] = existingConfigurationKeyValues[fqKey]
				}
			}
		}
	}
	return configurationKeyValues
}

// AppendDefaultValuesForMissingConfigurationValues adds default values for those keys that are not entered by the user
func AppendDefaultValuesForMissingConfigurationValues(spec domain.CloudflowApplicationSpec, configurationKeyValues map[string]string) map[string]string {
	for _, streamlet := range spec.Streamlets {
		for _, descriptor := range streamlet.Descriptor.ConfigParameters {
			fqKey := prefixWithStreamletName(streamlet.Name, descriptor.Key)
			if _, ok := configurationKeyValues[fqKey]; !ok {
				if len(descriptor.DefaultValue) > 0 {
					fmt.Printf("Default value '%s' will be used for configuration parameter '%s'\n", descriptor.DefaultValue, fqKey)
					configurationKeyValues[fqKey] = descriptor.DefaultValue
				}
			}
		}
	}
	return configurationKeyValues
}

// ValidateVolumeMounts validates that volume mounts command line arguments corresponds to a volume mount descriptor in the AD and that the PVC named in the argument exists
func ValidateVolumeMounts(k8sClient *kubernetes.Clientset, spec domain.CloudflowApplicationSpec, volumeMountPVCNameArray []string) (domain.CloudflowApplicationSpec, error) {
	// build a map of all user-specified volume mount arguments where the key
	// is [streamlet name].[volume mount key] and the value is the name of the
	// PVC to use for the mount.
	volumeMountPVCName := make(map[string]string)
	for _, e := range volumeMountPVCNameArray {
		keyValue := strings.Split(e, "=")
		if len(keyValue) != 2 {
			return spec, fmt.Errorf("\nInvalid volume mount parameter found.\n`%s` is not correctly formatted (`key=value`)", e)
		}
		volumeMountPVCName[keyValue[0]] = keyValue[1]
	}

	for _, streamlet := range spec.Streamlets {
		for m, mount := range streamlet.Descriptor.VolumeMounts {
			volumeMountName := prefixWithStreamletName(streamlet.Name, mount.Name)

			pvcName, ok := volumeMountPVCName[volumeMountName]
			if ok == false {
				return spec, fmt.Errorf("The following volume mount needs to be bound to a Persistence Volume claim using the --volume-mount flag\n\n- %s", volumeMountName)
			}

			pvc, err := k8sClient.CoreV1().PersistentVolumeClaims(spec.AppID).Get(pvcName, metav1.GetOptions{})
			if err != nil {
				return spec, fmt.Errorf("Persistent Volume Claim `%s` cannot be found in namespace `%s`", pvcName, spec.AppID)
			}

			if accessModeExists(pvc.Spec.AccessModes, mount.AccessMode) == false {
				return spec, fmt.Errorf("Persistent Volume Claim `%s` does not support requested access mode '%s'.\nThe PVC instead provides the following access modes: %v", pvcName, mount.AccessMode, pvc.Spec.AccessModes)
			}

			// We need to set the PVC on the associated streamlet deployment
			deploymentName := spec.AppID + "." + streamlet.Name
			for _, deployment := range spec.Deployments {
				if deployment.Name == deploymentName {
					for dm, deploymentMount := range deployment.VolumeMounts {
						if deploymentMount.Name == mount.Name {
							// Set the user-specified PVC name on the matching deployment in the spec (mutates the spec)
							deployment.VolumeMounts[dm].PVCName = pvcName
							// We also have to set it on the descriptor for backwards compatibility
							// with the operator, which breaks on CRs if we don't do this.
							streamlet.Descriptor.VolumeMounts[m].PVCName = pvcName

							fmt.Printf("\nThe following volume mount is now bound to Persistent Volume Claim `%s`:\n\n- %s\n\n", pvcName, volumeMountName)
						}
					}
				}
			}
		}
	}

	return spec, nil
}

func accessModeExists(accessModes []corev1.PersistentVolumeAccessMode, accessModeToFind string) bool {

	for i := range accessModes {
		if string(accessModes[i]) == accessModeToFind {
			return true
		}
	}
	return false
}

// ValidateConfigurationAgainstDescriptor validates all configuration parameter keys in the descriptor against a set of provided keys
func ValidateConfigurationAgainstDescriptor(spec domain.CloudflowApplicationSpec, configurationKeyValues map[string]string) (map[string]string, error) {

	type ValidationErrorDescriptor struct {
		FqKey              string
		ProblemDescription string
	}

	var missingKeys []ValidationErrorDescriptor
	var invalidKeys []ValidationErrorDescriptor

	for _, streamlet := range spec.Streamlets {
		for _, descriptor := range streamlet.Descriptor.ConfigParameters {
			fqKey := prefixWithStreamletName(streamlet.Name, descriptor.Key)

			if err := validateStreamletConfigKey(descriptor, configurationKeyValues[fqKey]); err != nil {
				invalidKeys = append(invalidKeys, ValidationErrorDescriptor{fqKey, err.Error()})
			}

			if _, ok := configurationKeyValues[fqKey]; !ok {
				missingKeys = append(missingKeys, ValidationErrorDescriptor{fqKey, descriptor.Description})
			}
		}
	}

	var str strings.Builder
	if len(missingKeys) > 0 {
		str.WriteString("Please provide values for the following configuration parameter(s):\n")
		for i := range missingKeys {
			str.WriteString(fmt.Sprintf("- %s - %s\n", missingKeys[i].FqKey, missingKeys[i].ProblemDescription))
		}
		return make(map[string]string), errors.New(str.String())
	}
	if len(invalidKeys) > 0 {
		str.WriteString("The following configuration parameter(s) have failed to validate:\n")
		for i := range invalidKeys {
			str.WriteString(fmt.Sprintf("- %s - %s\n", invalidKeys[i].FqKey, invalidKeys[i].ProblemDescription))
		}
		return make(map[string]string), errors.New(str.String())
	}
	return configurationKeyValues, nil
}

func validateStreamletConfigKey(descriptor domain.ConfigParameterDescriptor, value string) error {
	switch descriptor.Type {

	case "bool":
		if value != "true" && value != "false" &&
			value != "on" && value != "off" &&
			value != "yes" && value != "no" {
			return fmt.Errorf("Value `%s`is not a valid boolean. A boolean must be one of the following textual values `true','false',`yes`,`no`,`on` or `off`", value)
		}
	case "int32":
		_, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return fmt.Errorf("Value `%s` is not a valid integer.", value)
		}
	case "double":
		_, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("Value `%s` is not a valid double.", value)
		}
	case "string":
		r, err := regexp.Compile(descriptor.Pattern)
		if err != nil {
			return fmt.Errorf("The regular expression pattern failed to compile: %s", err.Error())
		}

		if !r.MatchString(value) {
			return fmt.Errorf("Value `%s` does not match the regular expression `%s`.", value, descriptor.Pattern)
		}
	case "duration":
		if err := util.ValidateDuration(value); err != nil {
			return err
		}
	case "memorysize":
		if err := util.ValidateMemorySize(value); err != nil {
			return err
		}
	default:
		return fmt.Errorf("Encountered an unknown validation type `%s`. Please make sure that the CLI is up-to-date.", descriptor.Type)
	}

	return nil
}

// CreateSecretsData creates a map of streamlet names and K8s Secrets for those streamlets with configuration parameters,
// the secrets contain a single key/value where the key is the name of the hocon configuration file
func CreateSecretsData(spec *domain.CloudflowApplicationSpec, configurationKeyValues map[string]string) map[string]*corev1.Secret {
	streamletSecretNameMap := make(map[string]*corev1.Secret)
	for _, streamlet := range spec.Streamlets {
		var str strings.Builder
		for _, descriptor := range streamlet.Descriptor.ConfigParameters {
			fqKey := formatStreamletConfigKeyFq(streamlet.Name, descriptor.Key)
			str.WriteString(fmt.Sprintf("%s=\"%s\"\r\n", fqKey, configurationKeyValues[prefixWithStreamletName(streamlet.Name, descriptor.Key)]))
		}
		secretMap := make(map[string]string)
		secretMap["secret.conf"] = str.String()
		secretName := findSecretName(spec, streamlet.Name)
		streamletSecretNameMap[secretName] = createSecret(spec.AppID, secretName, secretMap)
	}
	return streamletSecretNameMap
}

func findSecretName(spec *domain.CloudflowApplicationSpec, streamletName string) string {
	for _, deployment := range spec.Deployments {
		if deployment.StreamletName == streamletName {
			return deployment.SecretName
		}
	}
	panic(fmt.Errorf("Could not find secret name for streamlet %s", streamletName))
}

func createSecret(appID string, name string, data map[string]string) *corev1.Secret {
	labels := domain.CreateLabels(appID)
	labels["com.lightbend.cloudflow/streamlet-name"] = name
	secret := &corev1.Secret{
		Type: corev1.SecretTypeOpaque,
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: appID,
			Labels:    labels,
		},
	}
	secret.StringData = data
	return secret
}

func prefixWithStreamletName(streamletName string, key string) string {
	return fmt.Sprintf("%s.%s", streamletName, key)
}

func formatStreamletConfigKeyFq(streamletName string, key string) string {
	return fmt.Sprintf("cloudflow.streamlets.%s", prefixWithStreamletName(streamletName, key))
}

/*
{
  blueprint : {
    name : taxi-ride-fare
    images : {
      proc : docker.io/lightbend/processor:1.0
      ings : docker.io/lightbend/ingestor:1.0
      logr : docker.io/lightbend/logger:1.0
    }
    streamlets : {
      taxi-ride : ings/taxiride.ingestor.TaxiRideIngress
      taxi-fare : ings/taxiride.ingestor.TaxiFareIngress
      processor : proc/taxiride.processor.TaxiRideProcessor
      logger : logr/taxiride.logger.FarePerRideLogger
    }
    connections : {
      taxi-ride : {
        out : [processor.in-taxiride]
      }
      taxi-fare : {
        out : [processor.in-taxifare]
      }
      processor : {
        out : [logger.in]
      }
    }
  }
}
*/

// ImageReference is a destructured version of the image path
type ImageReference struct {
	Registry   string
	Repository string
	Image      string
	Tag        string
	FullURI    string
}

type blueprint struct {
	name        string
	images      []imageRef
	streamlets  []streamletRef
	connections connections
}

type imageRef struct {
	imageID  string
	imageURI string
}

type streamletRef struct {
	imageID     string
	streamletID string
	className   string
}

type streamletEndpoint struct {
	streamletID string
	port        string
}

type connections map[streamletEndpoint][]streamletEndpoint
