/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cloudflow.sbt

import java.io._
import java.nio.charset.StandardCharsets._

import spray.json._
import sbt._
import sbt.Keys._

import scala.util.control.NoStackTrace
import cloudflow.sbt.CloudflowKeys._
import cloudflow.blueprint.StreamletDescriptor
import cloudflow.blueprint.StreamletDescriptorFormat._
import cloudflow.blueprint.deployment.{ ApplicationDescriptor, CloudflowCR, Metadata, StreamletInstance }
import cloudflow.blueprint.deployment.CloudflowCRFormat.cloudflowCRFormat

/**
 * Plugin that generates the CR file for the application
 */
object CRGenerationPlugin extends AutoPlugin {
  final val TEMP_DIRECTORY = new File(System.getProperty("java.io.tmpdir"))
  
  override def requires =
    StreamletDescriptorsPlugin && BlueprintVerificationPlugin

  override def projectSettings = Seq(
    cloudflowApplicationCR := generateCR.dependsOn(verifyBlueprint).value
  )

  /**
   * Generate the Application CR from application descriptor generated by verification of
   * blueprint and the streamlet descriptors generated by the build process. We need the
   * latter since we need the proper image name associated with every streamlet descriptor
   * and streamlet deployment.
   */
  def generateCR: Def.Initialize[Task[Unit]] = Def.task {
    // these streamlet descriptors have been generated from the `build` task
    // if they have not been generated we throw an exception and ask the user
    // to run the build
    val log = streams.value.log

    val registry  = cloudflowDockerRegistry.value.get
    val namespace = cloudflowDockerRepository.value.get

    val streamletDescriptors = imageNamesByProject.value.foldLeft(Vector.empty[StreamletDescriptor]) {
      case (acc, (_, image)) =>
        val file = new File(TEMP_DIRECTORY, image.asTaggedName)
        if (file.exists()) {
          val json = new String(IO.readBytes(file), UTF_8)
          acc ++ json.parseJson.convertTo[Map[String, StreamletDescriptor]].values
        } else {
          acc
        }
    }

    // build is required before we can generate the CR
    if (streamletDescriptors.isEmpty) {
      throw new PreconditionViolationError("Need to run `build` first before generating CR")
    }

    // get the full image name because that's how we want in the application descriptor
    val streamletDescriptorsWithFullImageName = streamletDescriptors.map { streamletDescriptor =>
      streamletDescriptor.copy(image = s"$registry/$namespace/${streamletDescriptor.image}")
    }

    // this has been generated by `verifyBlueprint`
    val appDescriptor = applicationDescriptor.value.get

    // get the new streamlet descriptors to get the proper image name
    val newStreamletInstances = appDescriptor.streamlets.map { si =>
      val name = si.name
      val sd   = streamletDescriptorsWithFullImageName.find(_.className == si.descriptor.className).get
      StreamletInstance(name, sd)
    }

    // need to get the proper image name in `StreamletDeployment` s too
    val newDeployments = appDescriptor.deployments.map { deployment =>
      deployment.copy(image = streamletDescriptorsWithFullImageName.find(_.className == deployment.className).get.image)
    }

    // the new shiny `ApplicationDescriptor`
    val newApplicationDescriptor = appDescriptor.copy(streamlets = newStreamletInstances, deployments = newDeployments)

    // create the CR
    val cr = makeCR(newApplicationDescriptor)

    // generate the CR file in the current location
    val file = new File(s"${appDescriptor.appId}.json")
    IO.write(file, cr.toJson.compactPrint)
    log.success(s"Cloudflow application CR generated in ${file.name}")
  }

  def makeCR(appDescriptor: ApplicationDescriptor): CloudflowCR =
    CloudflowCR(
      apiVersion = "cloudflow.lightbend.com/v1alpha1",
      kind = "CloudflowApplication",
      metadata = Metadata(
        // @todo : need to change this version
        annotations = Map("com.lightbend.cloudflow/created-by-cli-version" -> "SNAPSHOT (local build)"),
        labels = Map(
          "app.kubernetes.io/managed-by"   -> "cloudflow",
          "app.kubernetes.io/part-of"      -> appDescriptor.appId,
          "com.lightbend.cloudflow/app-id" -> appDescriptor.appId
        ),
        name = appDescriptor.appId
      ),
      appDescriptor
    )

}

class PreconditionViolationError(msg: String) extends Exception(s"\n$msg") with NoStackTrace with sbt.FeedbackProvidedException
