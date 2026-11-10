import { defineConfig, devices } from '@playwright/test';
import { OSD_URL } from './stand';

/**
 * One shared Compose stand, so cases run one at a time in file order in one worker. No case is
 * retried: a retry would turn a flaky contract into a green run instead of a reported defect.
 */
export default defineConfig({
  testDir: '.',
  fullyParallel: false,
  workers: 1,
  retries: 0,
  forbidOnly: !!process.env.CI,
  timeout: 180_000,
  expect: { timeout: 30_000 },
  outputDir: 'test-results',
  reporter: [['list'], ['html', { open: 'never', outputFolder: 'playwright-report' }]],
  use: {
    ...devices['Desktop Chrome'],
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  projects: [
    {
      name: 'osd',
      testMatch: 'osd.spec.ts',
      use: { baseURL: OSD_URL, testIdAttribute: 'data-test-subj' },
    },
  ],
});
