import * as util from "util";
import * as ffmpeg from "fluent-ffmpeg";
import {
  Failure,
  failureTypeToMessage,
  TranscribeAudioSuccess,
  warningTypeToMessage,
} from "./types";
import { Channel } from "firebase-admin/eventarc";
import { google } from "@google-cloud/speech/build/protos/protos";

export function errorFromAny(anyErr: any): Error {
  let error: Error;
  if (!(anyErr instanceof Error)) {
    error = {
      name: "Thrown non-error object",
      message: String(anyErr),
    };
  } else {
    error = anyErr;
  }

  return error;
}

export function isNullFreeList<T>(
  list: (NonNullable<T> | null | undefined)[]
): list is NonNullable<T>[] {
  return list.every((item) => item != null);
}

export function getTaggedTranscriptOrNull(
  result: google.cloud.speech.v1.ISpeechRecognitionResult
): [number, string] | null {
  const channelTag = result?.channelTag;
  const transcription = result.alternatives?.[0].transcript;
  if (channelTag == null || transcription == null) {
    return null;
  }

  return [channelTag, transcription];
}

export function isTaggedStringArray(
  transcription?: (readonly [
    number | null | undefined,
    string | null | undefined
  ])[]
): transcription is [number, string][] {
  return (
    transcription != null &&
    transcription.every(([tag, string]) => tag != null && string != null)
  );
}

export function separateByTags(
  taggedStringList: [number, string][]
): Record<number, string[]> {
  return taggedStringList.reduce(
    (acc: Record<number, string[]>, [tag, string]) => {
      if (tag in acc) {
        acc[tag].push(string);
      } else {
        acc[tag] = [string];
      }
      return acc;
    },
    {}
  );
}

export const probePromise = util.promisify<string, ffmpeg.FfprobeData>(
  ffmpeg.ffprobe
);

export async function publishFailureEvent(
  eventChannel: Channel,
  { state, ...contents }: Failure
): Promise<void> {
  return eventChannel.publish({
    type: "firebase.extensions.storage-transcribe-audio.v1.fail",
    data: {
      ...contents,
    },
  });
}

export async function publishCompleteEvent(
  eventChannel: Channel,
  { state, ...contents }: TranscribeAudioSuccess
): Promise<void> {
  return eventChannel.publish({
    type: "firebase.extensions.storage-transcribe-audio.v1.complete",
    data: {
      ...contents,
    },
  });
}
