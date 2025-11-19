import { spawn, ChildProcess } from 'child_process';
import { Logger } from 'winston';
import path from 'path';
import fs from 'fs';

export interface PulseAudioRecorderOptions {
  userId: string;
  tempFileId: string;
  outputDir: string;
  sampleRate?: number;
  channels?: number;
  logger: Logger;
}

/**
 * PulseAudio backup recorder using parecord command
 * Records system audio as a backup to the primary MediaRecorder stream
 */
export class PulseAudioRecorder {
  private process: ChildProcess | null = null;
  private outputPath: string;
  private userId: string;
  private logger: Logger;
  private isRecording = false;
  private sampleRate: number;
  private channels: number;

  constructor(options: PulseAudioRecorderOptions) {
    this.userId = options.userId;
    this.logger = options.logger;
    this.sampleRate = options.sampleRate || 48000;
    this.channels = options.channels || 2;

    // Output file path: [outputDir]/[userId]/backup_[tempFileId].wav
    const userDir = path.join(options.outputDir, options.userId);
    this.outputPath = path.join(userDir, `backup_${options.tempFileId}.wav`);

    this.ensureOutputDirectory(userDir);
  }

  private ensureOutputDirectory(dir: string): void {
    try {
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
        this.logger.info(`Created PulseAudio backup directory: ${dir}`, { userId: this.userId });
      }
    } catch (error) {
      this.logger.error('Failed to create PulseAudio backup directory', { userId: this.userId, error });
      throw error;
    }
  }

  /**
   * Start recording with PulseAudio (parecord)
   * Uses the default source (system audio sink monitor)
   */
  public async startRecording(): Promise<void> {
    if (this.isRecording) {
      this.logger.warn('PulseAudio recorder already running', { userId: this.userId });
      return;
    }

    try {
      this.logger.info('Starting PulseAudio backup recorder', {
        userId: this.userId,
        outputPath: this.outputPath,
        sampleRate: this.sampleRate,
        channels: this.channels
      });

      // parecord command:
      // --file-format=wav : output as WAV
      // --rate=48000 : sample rate
      // --channels=2 : stereo
      // --device=@DEFAULT_MONITOR@ : record from default monitor (system audio)
      this.process = spawn('parecord', [
        '--file-format=wav',
        `--rate=${this.sampleRate}`,
        `--channels=${this.channels}`,
        '--device=@DEFAULT_MONITOR@',
        this.outputPath
      ], {
        stdio: ['ignore', 'pipe', 'pipe']
      });

      this.process.stdout?.on('data', (data: Buffer) => {
        this.logger.debug(`PulseAudio recorder stdout: ${data}`, { userId: this.userId });
      });

      this.process.stderr?.on('data', (data: Buffer) => {
        this.logger.warn(`PulseAudio recorder stderr: ${data}`, { userId: this.userId });
      });

      this.process.on('error', (error: Error) => {
        this.logger.error('PulseAudio recorder process error', { userId: this.userId, error });
        this.isRecording = false;
      });

      this.process.on('exit', (code: number | null, signal: NodeJS.Signals | null) => {
        this.logger.info('PulseAudio recorder process exited', {
          userId: this.userId,
          code,
          signal
        });
        this.isRecording = false;
      });

      this.isRecording = true;
      this.logger.info('PulseAudio backup recorder started successfully', { userId: this.userId });
    } catch (error) {
      this.logger.error('Failed to start PulseAudio backup recorder', { userId: this.userId, error });
      throw error;
    }
  }

  /**
   * Stop the PulseAudio recording gracefully
   */
  public async stopRecording(): Promise<void> {
    if (!this.isRecording || !this.process) {
      this.logger.warn('PulseAudio recorder not running or already stopped', { userId: this.userId });
      return;
    }

    return new Promise((resolve) => {
      this.logger.info('Stopping PulseAudio backup recorder', { userId: this.userId });

      const timeout = setTimeout(() => {
        if (this.process && !this.process.killed) {
          this.logger.warn('PulseAudio process did not stop gracefully, forcing kill', { userId: this.userId });
          this.process.kill('SIGKILL');
        }
        resolve();
      }, 5000);

      this.process!.once('exit', () => {
        clearTimeout(timeout);
        this.isRecording = false;
        this.logger.info('PulseAudio backup recorder stopped', {
          userId: this.userId,
          outputPath: this.outputPath
        });
        resolve();
      });

      // Send SIGINT for graceful shutdown (parecord handles this properly)
      this.process!.kill('SIGINT');
    });
  }

  /**
   * Get the output file path
   */
  public getOutputPath(): string {
    return this.outputPath;
  }

  /**
   * Check if the backup file exists and has content
   */
  public async hasValidRecording(): Promise<boolean> {
    try {
      const stats = await fs.promises.stat(this.outputPath);
      const hasContent = stats.size > 1024; // At least 1KB
      this.logger.info('PulseAudio backup file check', {
        userId: this.userId,
        exists: true,
        size: stats.size,
        hasContent
      });
      return hasContent;
    } catch (error) {
      this.logger.warn('PulseAudio backup file not found or inaccessible', {
        userId: this.userId,
        outputPath: this.outputPath
      });
      return false;
    }
  }

  /**
   * Delete the backup recording file
   */
  public async deleteRecording(): Promise<void> {
    try {
      if (fs.existsSync(this.outputPath)) {
        await fs.promises.unlink(this.outputPath);
        this.logger.info('Deleted PulseAudio backup recording', {
          userId: this.userId,
          path: this.outputPath
        });
      }
    } catch (error) {
      this.logger.warn('Failed to delete PulseAudio backup recording', {
        userId: this.userId,
        error
      });
    }
  }

  /**
   * Check if PulseAudio is available on the system
   */
  public static async checkPulseAudioAvailable(logger: Logger): Promise<boolean> {
    return new Promise((resolve) => {
      const check = spawn('which', ['parecord']);
      check.on('exit', (code: number | null) => {
        const available = code === 0;
        logger.info('PulseAudio availability check', { available });
        resolve(available);
      });
      check.on('error', () => {
        logger.warn('PulseAudio check failed - parecord not found');
        resolve(false);
      });
    });
  }
}
