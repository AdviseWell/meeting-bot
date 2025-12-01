import { test, expect } from '@playwright/test';
import { promises as fs } from 'fs';

declare global {
  interface Window {
    audioContext: AudioContext;
    oscillator: OscillatorNode;
    gainNode: GainNode;
    mediaRecorder: MediaRecorder;
    recordedChunks: Blob[];
    audioBlob?: Blob;
    audioBlobUrl?: string;
  }
}

test('browser audio capture test', async ({ page }) => {
  test.setTimeout(30000);

  // Enable console logging from the page
  page.on('console', msg => console.log(`PAGE LOG: ${msg.text()}`));

  // Set up audio context and capture
  // Navigate to a secure context (HTTPS) to ensure mediaDevices API is available
  await page.goto('https://example.com');
  await page.setContent('<html><body><h1>Audio Capture Test</h1><button id="start">Start Audio</button><button id="stop">Stop Audio</button><div id="status">Ready</div></body></html>');

  // Create audio context and media recorder for capturing audio
  await page.evaluate(async () => {
    console.log('Creating audio context...');
    const audioContext = new AudioContext();
    const oscillator = audioContext.createOscillator();
    const gainNode = audioContext.createGain();
    const streamDestination = audioContext.createMediaStreamDestination();

    oscillator.connect(gainNode);
    gainNode.connect(streamDestination);
    // Connect to destination for playback (optional, but good for debugging)
    gainNode.connect(audioContext.destination);

    oscillator.frequency.setValueAtTime(440, audioContext.currentTime); // A4 note
    oscillator.type = 'sine';
    gainNode.gain.setValueAtTime(0.1, audioContext.currentTime); // Lower volume

    console.log('Audio context created, playing audio...');
    oscillator.start();

    // Create a dummy video track from canvas to simulate screen share
    // This avoids using getDisplayMedia which can be flaky in test environments
    const canvas = document.createElement('canvas');
    canvas.width = 640;
    canvas.height = 480;
    const ctx = canvas.getContext('2d');
    if (ctx) {
      ctx.fillStyle = 'red';
      ctx.fillRect(0, 0, 640, 480);
    }
    const videoStream = canvas.captureStream(30);
    const videoTrack = videoStream.getVideoTracks()[0];

    // Use the stream from the destination (audio) and add video
    const stream = streamDestination.stream;
    stream.addTrack(videoTrack);
    console.log('Created synthetic stream with video and audio');

    console.log('Got stream, tracks:', stream.getTracks().length);
    const audioTracks = stream.getAudioTracks();
    console.log('Audio tracks:', audioTracks.length);

    if (audioTracks.length === 0) {
      throw new Error('No audio tracks found in stream');
    }

    // Set up MediaRecorder to capture the audio
    const recordedChunks: Blob[] = [];
    const mediaRecorder = new MediaRecorder(stream, { mimeType: 'video/webm' }); // Usually video/webm for display media

    mediaRecorder.ondataavailable = (event) => {
      console.log('Data available event:', event.data.size, 'bytes');
      if (event.data.size > 0) {
        recordedChunks.push(event.data);
      }
    };

    mediaRecorder.onstop = () => {
      console.log('Recording stopped, chunks:', recordedChunks.length);
      const blob = new Blob(recordedChunks, { type: 'audio/wav' });
      console.log('Created blob of size:', blob.size);

      // Create download link and trigger download
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'captured_audio.wav';
      a.style.display = 'none';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);

      // Store blob for validation
      window.audioBlob = blob;
      window.audioBlobUrl = url;

      console.log('Audio download triggered and blob stored');
    };

    // Store references globally
    window.audioContext = audioContext;
    window.oscillator = oscillator;
    window.gainNode = gainNode;
    window.mediaRecorder = mediaRecorder;
    window.recordedChunks = recordedChunks;

    // Update status
    const statusEl = document.getElementById('status');
    if (statusEl) {
      statusEl.textContent = 'Audio context and recorder created';
    }
  });

  // Start recording and audio
  await page.evaluate(() => {
    window.mediaRecorder.start(1000); // Request data every second
    // Oscillator already started in setup
    const statusEl = document.getElementById('status');
    if (statusEl) {
      statusEl.textContent = 'Recording audio (440Hz sine wave)';
    }
  });

  // Wait for audio recording period
  await page.waitForTimeout(5000); // Increased to 5 seconds

  // Setup download listener BEFORE stopping recording to avoid race condition
  const downloadPromise = page.waitForEvent('download');

  // Stop recording and audio
  await page.evaluate(() => {
    window.oscillator.stop();
    window.mediaRecorder.stop();
    const statusEl = document.getElementById('status');
    if (statusEl) {
      statusEl.textContent = 'Audio recording completed';
    }
  });

  // Wait for download to complete
  const download = await downloadPromise;

  // Save the downloaded file
  const downloadPath = '/tmp/captured_audio.wav';
  await download.saveAs(downloadPath);
  console.log('Audio file saved to:', downloadPath);

  // Wait for the recording to complete
  await page.waitForTimeout(500);

  // Check if audio was captured and downloaded
  const audioResult = await page.evaluate(() => {
    return {
      blobSize: window.audioBlob?.size || 0,
      hasBlob: !!window.audioBlob,
      chunksCount: window.recordedChunks?.length || 0
    };
  });

  console.log('Audio capture result:', audioResult);

  // Verify audio was captured
  expect(audioResult.hasBlob).toBe(true);
  expect(audioResult.blobSize).toBeGreaterThan(0);
  expect(audioResult.chunksCount).toBeGreaterThan(0);

  // Verify the downloaded file exists and has content
  const filePath = '/tmp/captured_audio.wav';
  try {
    const stats = await fs.stat(filePath);
    expect(stats.size).toBeGreaterThan(0);
    console.log('Downloaded file size:', stats.size, 'bytes');
  } catch (error) {
    throw new Error(`Downloaded file not found or empty: ${filePath}`);
  }

  // Verify the page loaded and audio test ran
  await expect(page.locator('#status')).toContainText('completed');
});
