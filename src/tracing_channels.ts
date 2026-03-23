/*
 * @boringnode/queue
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

import diagnostics_channel from 'node:diagnostics_channel'
import type { JobDispatchMessage, JobExecuteMessage } from './types/tracing_channels.js'

/**
 * Traces job dispatch operations (push to queue).
 * Fires for single dispatch, batch dispatch, and scheduled job dispatch.
 */
export const dispatchChannel = diagnostics_channel.tracingChannel<
  'boringqueue.job.dispatch',
  JobDispatchMessage
>('boringqueue.job.dispatch')

/**
 * Traces job execution by the worker or sync adapter.
 * Each retry attempt fires a separate trace.
 */
export const executeChannel = diagnostics_channel.tracingChannel<
  'boringqueue.job.execute',
  JobExecuteMessage
>('boringqueue.job.execute')
