import worker from './index';
import { withStatusSnapshot } from './status-snapshot';

export default withStatusSnapshot(worker, {
  name: 'TrendPulse',
  store: env => env.TP_DATA,
  maxAgeMs: 8 * 60 * 60 * 1000,
});
