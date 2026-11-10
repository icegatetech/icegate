import { defineConfig, devices } from '@playwright/test';
import { GRAFANA_URL, OSD_URL } from './stand';

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
    {
      // The Grafana project keeps Playwright's default test-id attribute, `data-testid`.
      name: 'grafana',
      testMatch: 'grafana.spec.ts',
      use: { baseURL: GRAFANA_URL },
    },
    {
      // Each case resets the stand and starts the variant it needs, so this project runs alone
      // and after the others; its cases address each service by absolute URL. Like the Grafana
      // project, it keeps Playwright's default test-id attribute, `data-testid`.
      name: 'versions',
      testMatch: 'versions.spec.ts',
      // Above the sum of the bounded waits of the longest case (the strict-mode case), so a slow
      // boot fails on its own named poll rather than on the test timeout.
      timeout: 1_800_000,
    },
  ],
});
