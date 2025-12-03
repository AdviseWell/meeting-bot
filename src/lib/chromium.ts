import { Browser, BrowserContext, Page } from 'playwright';
import { chromium } from 'playwright-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import config from '../config';
import { getCorrelationIdLog } from '../util/logger';

const stealthPlugin = StealthPlugin();
stealthPlugin.enabledEvasions.delete('iframe.contentWindow');
stealthPlugin.enabledEvasions.delete('media.codecs');
chromium.use(stealthPlugin);

function attachBrowserErrorHandlers(browser: Browser, context: BrowserContext, page: Page, correlationId: string) {
  const log = getCorrelationIdLog(correlationId);

  browser.on('disconnected', () => {
    console.log(`${log} Browser has disconnected!`);
  });

  context.on('close', () => {
    console.log(`${log} Browser has closed!`);
  });

  page.on('crash', (page) => {
    console.error(`${log} Page has crashed! ${page?.url()}`);
  });

  page.on('close', (page) => {
    console.log(`${log} Page has closed! ${page?.url()}`);
  });
}

async function launchBrowserWithTimeout(launchFn: () => Promise<Browser>, timeoutMs: number, correlationId: string): Promise<Browser> {
  let timeoutId: NodeJS.Timeout;
  let finished = false;

  return new Promise((resolve, reject) => {
    // Set up timeout
    timeoutId = setTimeout(() => {
      if (!finished) {
        finished = true;
        reject(new Error(`Browser launch timed out after ${timeoutMs}ms`));
      }
    }, timeoutMs);

    // Start launch
    launchFn()
      .then(result => {
        if (!finished) {
          finished = true;
          clearTimeout(timeoutId);
          console.log(`${getCorrelationIdLog(correlationId)} Browser launch function success!`);
          resolve(result);
        }
      })
      .catch(err => {
        console.error(`${getCorrelationIdLog(correlationId)} Error launching browser`, err);
        if (!finished) {
          finished = true;
          clearTimeout(timeoutId);
          reject(err);
        }
      });
  });
}

async function createBrowserContext(url: string, correlationId: string): Promise<Page> {
  const size = { width: 1280, height: 720 };

  const browserArgs: string[] = [
    '--enable-usermedia-screen-capturing',
    '--allow-http-screen-capture',
    '--auto-select-desktop-capture-source=Entire screen',
    '--auto-select-tab-capture-source-by-title=Microsoft Teams',
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-web-security',
    '--disable-gpu',  // Disable GPU on headless/K8s environments
    '--disable-software-rasterizer',  // Use CPU rasterizer optimized for headless
    `--window-size=${size.width},${size.height}`,
    '--auto-accept-this-tab-capture',
    '--enable-features=MediaRecorder',
    // Audio capture flags - CRITICAL for getDisplayMedia audio
    '--autoplay-policy=no-user-gesture-required',
    '--allow-running-insecure-content',
    // Enable hardware audio processing for better performance
    '--enable-features=AudioServiceOutOfProcess',  // Run audio in separate process
    // Disable crash reporting to prevent SIGTRAP issues
    '--disable-crash-reporter',
    '--disable-breakpad',
    // Performance optimizations
    '--disable-extensions',
    '--disable-component-extensions-with-background-pages',
    '--disable-background-networking',
    '--disable-sync',
    '--disable-translate',
    '--disable-features=TranslateUI',
    '--no-first-run',
    '--no-default-browser-check',
    '--disable-backgrounding-occluded-windows',
    '--disable-renderer-backgrounding',
    '--disable-background-timer-throttling',
    '--disable-hang-monitor',
    '--disable-prompt-on-repost',
    '--disable-domain-reliability',
    '--disable-component-update',
    // CRITICAL: Create virtual mic and camera (required for K8s audio to work)
    '--use-fake-device-for-media-stream',
    // Auto-accept permission prompts for ALLOWED permissions only
    '--use-fake-ui-for-media-stream',
  ];

  const browser = await launchBrowserWithTimeout(
    async () => await chromium.launch({
      headless: false,
      args: browserArgs,
      ignoreDefaultArgs: ['--mute-audio'],
      executablePath: config.chromeExecutablePath,
    }),
    60000,
    correlationId
  );

  const linuxX11UserAgent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36';

  const context = await browser.newContext({
    permissions: ['microphone'],
    viewport: size,
    ignoreHTTPSErrors: true,
    userAgent: linuxX11UserAgent,
  });

  await context.grantPermissions(['microphone'], { origin: url });

  const page = await context.newPage();

  // Attach common error handlers
  attachBrowserErrorHandlers(browser, context, page, correlationId);

  console.log(`${getCorrelationIdLog(correlationId)} Browser launched successfully!`);

  return page;
}

export default createBrowserContext;
