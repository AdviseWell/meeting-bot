import { Page } from 'playwright';
import { AbstractMeetBot, JoinParams } from './AbstractMeetBot';
import { UnsupportedMeetingError, WaitingAtLobbyError } from '../error';
import { addBotLog } from '../services/botService';
import { Logger } from 'winston';
import { LogSubCategory, UnsupportedMeetingCategory, WaitingAtLobbyCategory } from '../types';
import { GOOGLE_REQUEST_DENIED, MICROSOFT_REQUEST_DENIED, ZOOM_REQUEST_DENIED } from '../constants';
import { IUploader } from '../middleware/disk-uploader';
import { vp9MimeType, webmMimeType } from '../lib/recording';
import config from '../config';

export class MeetBotBase extends AbstractMeetBot {
  protected page: Page;
  protected slightlySecretId: string; // Use any hard-to-guess identifier
  protected earlyRecordingActive = false;

  join(params: JoinParams): Promise<void> {
    throw new Error('Function not implemented.');
  }

  /**
   * Starts recording immediately upon page load, before joining the meeting.
   * This captures the pre-join screen and any early meeting content.
   */
  protected async startEarlyRecording(params: {
    teamId: string;
    userId: string;
    eventId?: string;
    botId?: string;
    uploader: IUploader;
  }, logger: Logger): Promise<boolean> {
    try {
      logger.info('Starting early recording before join...');

      // Set up the data handler for recording chunks
      await this.page.exposeFunction(
        'screenAppSendData',
        async (slightlySecretId: string, data: string) => {
          if (slightlySecretId !== this.slightlySecretId) return;
          const buffer = Buffer.from(data, 'base64');
          await params.uploader.saveDataToTempFile(buffer);
        }
      );

      // Set up the meeting end handler
      await this.page.exposeFunction(
        'screenAppMeetEnd',
        (slightlySecretId: string) => {
          if (slightlySecretId !== this.slightlySecretId) return;
          logger.info('Early recording end event received');
        }
      );

      // Start the actual recording
      const recordingStarted = await this.page.evaluate(
        async ({
          slightlySecretId,
          primaryMimeType,
          secondaryMimeType,
        }: {
          slightlySecretId: string;
          primaryMimeType: string;
          secondaryMimeType: string;
        }) => {
          try {
            if (
              !navigator.mediaDevices ||
              !navigator.mediaDevices.getDisplayMedia
            ) {
              console.error('MediaDevices API not supported');
              return false;
            }

            // Request display media with audio
            const stream: MediaStream = await (
              navigator.mediaDevices as any
            ).getDisplayMedia({
              video: {
                frameRate: { ideal: 30, max: 60 }
              },
              audio: {
                autoGainControl: false,
                channels: 2,
                channelCount: 2,
                echoCancellation: false,
                noiseSuppression: false,
                sampleRate: 48000,
                sampleSize: 16,
              },
              preferCurrentTab: true,
            });

            // Determine best mime type
            let options: MediaRecorderOptions = {};
            if (MediaRecorder.isTypeSupported(primaryMimeType)) {
              console.log(`Early recording using ${primaryMimeType}`);
              options = {
                mimeType: primaryMimeType,
                videoBitsPerSecond: 15000000,
                audioBitsPerSecond: 768000
              };
            } else if (MediaRecorder.isTypeSupported(secondaryMimeType)) {
              console.log(`Early recording using fallback ${secondaryMimeType}`);
              options = {
                mimeType: secondaryMimeType,
                videoBitsPerSecond: 15000000,
                audioBitsPerSecond: 768000
              };
            } else {
              console.error('No supported mime type found for early recording');
              stream.getTracks().forEach((track) => track.stop());
              return false;
            }

            // Create media recorder
            const mediaRecorder = new MediaRecorder(stream, { ...options });

            mediaRecorder.ondataavailable = async (event: BlobEvent) => {
              if (!event.data || !event.data.size) return;
              try {
                const arrayBuffer = await event.data.arrayBuffer();
                function arrayBufferToBase64(buffer: ArrayBuffer) {
                  let binary = '';
                  const bytes = new Uint8Array(buffer);
                  for (let i = 0; i < bytes.byteLength; i++) {
                    binary += String.fromCharCode(bytes[i]);
                  }
                  return btoa(binary);
                }
                const base64 = arrayBufferToBase64(arrayBuffer);
                await (window as any).screenAppSendData(
                  slightlySecretId,
                  base64
                );
              } catch (error) {
                console.error('Error uploading early recording chunk:', error);
              }
            };

            mediaRecorder.onerror = (event: any) => {
              console.error('Early recording MediaRecorder error:', event.error);
            };

            // Start recording with 2-second chunks
            mediaRecorder.start(2000);

            // Store references globally for later access
            (window as any).__earlyMediaRecorder = mediaRecorder;
            (window as any).__earlyStream = stream;
            (window as any).__earlyRecordingStartTime = Date.now();

            console.log('Early recording started successfully at', new Date().toISOString());
            return true;
          } catch (error) {
            console.error('Failed to start early recording:', error);
            return false;
          }
        },
        {
          slightlySecretId: this.slightlySecretId,
          primaryMimeType: webmMimeType,
          secondaryMimeType: vp9MimeType,
        }
      );

      if (recordingStarted) {
        this.earlyRecordingActive = true;
        logger.info('Early recording started successfully');
      } else {
        logger.warn('Early recording failed to start, will fall back to normal recording');
      }

      return recordingStarted;
    } catch (error) {
      logger.error('Error setting up early recording:', error);
      return false;
    }
  }

  /**
   * Sets up monitoring for an already-active early recording session.
   * Adds timeout and inactivity detection without starting a new recording.
   */
  protected async setupRecordingMonitoring(params: {
    duration: number;
    inactivityLimit: number;
    userId: string;
    teamId: string;
  }, logger: Logger): Promise<void> {
    const { duration, inactivityLimit, userId, teamId } = params;

    await this.page.evaluate(
      ({
        duration,
        inactivityLimit,
        userId,
        teamId,
        slightlySecretId,
        activateInactivityDetectionAfterMinutes,
      }) => {
        const mediaRecorder = (window as any).__earlyMediaRecorder;
        const stream = (window as any).__earlyStream;

        if (!mediaRecorder || !stream) {
          console.error('Early recording objects not found for monitoring setup');
          return;
        }

        console.log('Setting up recording monitoring for early recording...');

        const stopTheRecording = async () => {
          console.log('Stopping early recording...');
          try {
            if (mediaRecorder.state !== 'inactive') {
              mediaRecorder.stop();
            }
            stream.getTracks().forEach((track: any) => track.stop());
            (window as any).screenAppMeetEnd(slightlySecretId);
          } catch (error) {
            console.error('Error stopping early recording:', error);
          }
        };

        // Set up max duration timeout
        setTimeout(() => {
          console.log('Max duration reached, stopping early recording');
          stopTheRecording();
        }, duration);

        // Set up inactivity detection after delay
        setTimeout(() => {
          console.log('Activating inactivity detection for early recording...');

          let lastActivity = Date.now();
          const activityEvents = [
            'mousemove',
            'keydown',
            'click',
            'scroll',
            'touchstart',
          ];

          const updateActivity = () => {
            lastActivity = Date.now();
          };

          activityEvents.forEach((event) => {
            document.addEventListener(event, updateActivity, { passive: true });
          });

          // Check for inactivity every 30 seconds
          const inactivityCheckInterval = setInterval(() => {
            const inactiveTime = Date.now() - lastActivity;
            if (inactiveTime > inactivityLimit) {
              console.log(
                `Inactivity detected (${Math.round(inactiveTime / 1000)}s), stopping early recording`
              );
              clearInterval(inactivityCheckInterval);
              stopTheRecording();
            }
          }, 30000);

          // Store interval reference for cleanup
          (window as any).__inactivityCheckInterval = inactivityCheckInterval;
        }, activateInactivityDetectionAfterMinutes * 60 * 1000);

        console.log('Early recording monitoring active');
      },
      {
        duration,
        inactivityLimit,
        userId,
        teamId,
        slightlySecretId: this.slightlySecretId,
        activateInactivityDetectionAfterMinutes:
          config.activateInactivityDetectionAfter,
      }
    );

    logger.info('Early recording monitoring configured');
  }
}

export const handleWaitingAtLobbyError = async ({
  provider,
  eventId,
  botId,
  token,
  error,
}: {
  eventId?: string,
  token: string,
  botId?: string,
  provider: 'google' | 'microsoft' | 'zoom',
  error: WaitingAtLobbyError,
}, logger: Logger) => {
  const getSubCategory = (provider: 'google' | 'microsoft' | 'zoom', bodytext: string | undefined | null): WaitingAtLobbyCategory['subCategory'] => {
    switch (provider) {
      case 'google':
        return bodytext?.includes(GOOGLE_REQUEST_DENIED) ? 'UserDeniedRequest' : 'Timeout';
      case 'microsoft':
        return bodytext?.includes(MICROSOFT_REQUEST_DENIED) ? 'UserDeniedRequest' : 'Timeout';
      case 'zoom':
        return bodytext?.includes(ZOOM_REQUEST_DENIED) ? 'UserDeniedRequest' : 'Timeout';
      default:
        return 'Timeout';
    }
  };

  const bodytext = error.documentBodyText;
  const subCategory = getSubCategory(provider, bodytext);

  const result = await addBotLog({
    level: 'error',
    message: error.message,
    provider,
    token,
    botId,
    eventId,
    category: 'WaitingAtLobby',
    subCategory: subCategory,
  }, logger);
  return result;
};

export const handleUnsupportedMeetingError = async ({
  provider,
  eventId,
  botId,
  token,
  error,
}: {
  eventId?: string,
  token: string,
  botId?: string,
  provider: 'google' | 'microsoft' | 'zoom',
  error: UnsupportedMeetingError,
}, logger: Logger) => {
  const getSubCategory = (error: UnsupportedMeetingError): null | LogSubCategory<'UnsupportedMeeting'> => {
    if (error.googleMeetPageStatus === 'SIGN_IN_PAGE') {
      return 'RequiresSignIn';
    }
    return null;
  };

  const category: UnsupportedMeetingCategory['category'] = 'UnsupportedMeeting';
  const subCategory = getSubCategory(error);
  if (!subCategory) {
    return;
  }

  const result = await addBotLog({
    level: 'error',
    message: error.message,
    provider,
    token,
    botId,
    eventId,
    category,
    subCategory: subCategory,
  }, logger);
  return result;
};
