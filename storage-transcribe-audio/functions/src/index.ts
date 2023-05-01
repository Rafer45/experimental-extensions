/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import { getEventarc, Channel } from "firebase-admin/eventarc";
import * as speech from "@google-cloud/speech";
import * as path from "path";
import * as os from "os";

import * as logs from "./logs";
import {
  publishFailureEvent,
  errorFromAny,
  publishCompleteEvent,
  generateTempTranscodedFilename,
} from "./util";
import mkdirp = require("mkdirp");
import {
  transcodeToLinear16,
  transcribeAndUpload,
  uploadTranscodedFile,
} from "./transcribe-audio";
import { Status } from "./types";
import config from "./config";

admin.initializeApp();

const eventChannel: Channel | null = process.env.EVENTARC_CHANNEL
  ? getEventarc().channel(process.env.EVENTARC_CHANNEL, {
      allowedEventTypes: process.env.EXT_SELECTED_EVENTS,
    })
  : null;

logs.init();

const client = new speech.SpeechClient();

// TODO(reao): Write to firestore if that setting is enabled
// TODO(reao): Don't write to storage if that setting is not enabled
export const transcribeAudio = functions.storage
  .object()
  .onFinalize(async (object): Promise<void> => {
    logs.start();

    if (!isValidObject(object)) {
      logs.objectSkipped(object)
      return;
    }

    const bucket = admin.storage().bucket(object.bucket);
    const bucketPath = object.name;
    const base = path.basename(bucketPath)

    try {
      const localCopyPath: string = path.join(os.tmpdir(), base);
      const tempLocalDir = path.dirname(localCopyPath);

      logs.tempDirectoryCreating(tempLocalDir);
      await mkdirp(tempLocalDir);
      logs.tempDirectoryCreated(tempLocalDir);

      const remoteFile = bucket.file(bucketPath);
      logs.audioDownloading(bucketPath);
      await remoteFile.download({ destination: localCopyPath });
      logs.audioDownloaded(bucketPath, localCopyPath);

      const transcodeResult = await transcodeToLinear16(localCopyPath);

      if (transcodeResult.status == Status.FAILURE) {
        logs.transcodingFailed(transcodeResult);
        if (eventChannel) {
          await publishFailureEvent(eventChannel, transcodeResult);
        }
        return;
      }

      logs.debug("uploading transcoded file");
      const tempFilename = generateTempTranscodedFilename(new Date(), base);
      const transcodedUploadResult = await uploadTranscodedFile({
        localPath: transcodeResult.localOutputPath,
        storagePath: path.join(config.outputCollection || bucketPath, tempFilename),
        bucket: bucket,
      })
      if (transcodedUploadResult.status == Status.FAILURE) {
        logs.transcodeUploadFailed(transcodedUploadResult);
        if (eventChannel) {
          await publishFailureEvent(eventChannel, transcodedUploadResult);
        }
        return;
      }
      logs.debug("uploaded transcoded file");

      const { sampleRateHertz, audioChannelCount } = transcodeResult;
      const [file, /*, metadata */] = transcodedUploadResult.uploadResponse;

      const transcriptionResult = await transcribeAndUpload({
        client,
        file,
        sampleRateHertz,
        audioChannelCount,
      });

      if (transcriptionResult.status == Status.FAILURE) {
        logs.transcribingFailed(transcriptionResult);
        if (eventChannel) {
          await publishFailureEvent(eventChannel, transcriptionResult);
        }
        return;
      }

      if (eventChannel) {
        await publishCompleteEvent(eventChannel, transcriptionResult);
      }
      return;
    } catch (err) {
      const error = errorFromAny(err);
      logs.error(error);

      if (eventChannel) {
        await eventChannel.publish({
          type: "firebase.extensions.storage-transcribe-audio.v1.fail",
          data: {
            error,
          },
        });
      }
    }
  });

function isValidObject(object: functions.storage.ObjectMetadata): object is (functions.storage.ObjectMetadata & {name: string}) {
  const { contentType } = object; // the MIME type

  if (object.metadata && object.metadata.isTranscodeOutput === "true") {
    logs.audioAlreadyProcessed();
    return false;
  }

  if (!contentType) {
    logs.noContentType();
    return false;
  }

  if (!contentType.startsWith("audio/")) {
    logs.contentTypeInvalid(contentType);
    return false;
  }

  if (object.name === undefined) {
    logs.undefinedObjectName(object);
    return false;
  }

  return true
}