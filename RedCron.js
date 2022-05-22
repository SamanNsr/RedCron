const { promisify } = require('util');

const _ = require('lodash');
const CronJob = require('node-cron');
const cronParser = require('cron-parser');
const RedLock = require('redlock');

const { logger } = require('../config');

/**
 * Executes the action for either onetime or many times, using lock and redis to synchronize
 * execution.
 *
 * @param  {Function}       action   The function to be executed on the provided schedule
 * @param  {schedule}       schedule A cron expression schedule for the action
 * @param  {string}         jobName  The name of the job to be executed
 * @private
 */
async function executeAction(action, schedule, jobName) {
  const redisGet = promisify(this.redisClient.get).bind(this.redisClient);
  const redisSet = promisify(this.redisClient.set).bind(this.redisClient);
  let actionFired = false;
  let nextExecution;
  let reply;
  let now;

  // Stage 1: get the data from redis
  try {
    // Gets the job next execution saved on redis
    reply = await redisGet(jobName);
    /*
     * If the next execution (saved on redis) is null or greater than now, then this job must
     * execute, else, it was already executed
     */
    const interval = cronParser.parseExpression(schedule);
    const nextExecutions = [];
    nextExecutions[0] = interval.next().toDate();
    nextExecutions[1] = interval.next().toDate();

    // Sets milliseconds to zero since cron expression doesn't use or set it
    _.forEach(nextExecutions, (execution) => execution.setMilliseconds(0));

    now = new Date().getTime();

    nextExecution = now >= nextExecutions[0].getTime() ? nextExecutions[1] : nextExecutions[0];
  } catch (e) {
    this.logger.error(e);
    return;
  }

  // Stage 2: lock the job and execute the action
  try {
    // Locks so other jobs wait before executing
    await this.redisLock.lock(`locks:${jobName}`, 750);
    if (_.isNil(reply) || reply.toString() < now) {
      // Calls the method passed by the user
      actionFired = true;
      try {
        action();
      } catch (error) {
        this.logger.error(error);
      }
    }
  } catch (e) {
    return;
  }

  // Stage 3: save the next execution on redis
  try {
    if (actionFired) {
      // Sets the next execution time on redis so other jobs wont run
      await redisSet(jobName, nextExecution.getTime());
    }
  } catch (e) {
    this.logger.error(e);
  }
}

/**
 * Class to schedule synchronized jobs.
 */
class RedCron {
  /**
   * Sets up the class, saving the redis client and using it to startup redisLock.
   *
   * @param {RedisClient} client       A redis client connected to a redis instance where the jobs
   *                                   data will be saved
   * @param {Logger}      [userLogger] A user provided logger to be used to log errors
   */
  constructor(client, userLogger) {
    this.redisClient = client;
    // Uses the provided client to configure redisLock
    this.redisLock = new RedLock([this.redisClient], { retryCount: 0 });
    // Initialize jobs list
    this.jobs = {};
    // Use provided logger or default
    this.logger = userLogger || logger;

    this.redisLock.on('error', (error) => {
      // Ignore cases where a resource is explicitly marked as locked on a client.
      if (error instanceof RedLock.LockError) {
        console.log('Lock error');
        return;
      }
      // Log all other errors.
      this.logger.error(error);
    });
  }

  /**
   * Executes the provided function repeatedly in the provided schedule. Uses the job name as a key
   * on redis, therefore it must be unique.
   *
   * @param  {Function}       action   The function to be executed on the provided schedule
   * @param  {schedule}       schedule A cron expression schedule for the action
   * @param  {string}         jobName  The name of the job to be executed
   */
  addJob(action, schedule, jobName) {
    // Save the job timer so it can be cancelled
    const job = CronJob.schedule(schedule, () => {
      executeAction.bind(this)(action, schedule, jobName);
    });
    _.set(this.jobs, `${jobName}.timer`, job);

    // Save the method action so it can be ran manually
    this.jobs[jobName].action = action;
  }

  /**
   * Executes the provided function once in the provided schedule. Uses the job name as a key on
   * redis, therefore it must be unique.
   *
   * @param  {Function}       action   The function to be executed on the provided schedule
   * @param  {schedule}       schedule A cron expression schedule for the action
   * @param  {string}         jobName  The name of the job to be executed
   */
  addSingleExecutionJob(action, schedule, jobName) {
    // Save the job timer so it can be cancelled
    _.set(
      this.jobs,
      `${jobName}.timer`,
      CronJob.schedule(schedule, () => {
        executeAction.bind(this)(action, schedule, jobName);
      }).once(),
    );
    // Save the method action so it can be ran manually
    this.jobs[jobName].action = action;
  }

  /**
   * Cancels a jobs execution, clearing all data about it.
   *
   * @param  {string} jobName The name of the job to be cancelled.
   */
  cancelJob(jobName) {
    const redisDel = promisify(this.redisClient.del).bind(this.redisClient);
    if (this.jobs[jobName]) {
      this.jobs[jobName].timer.clear();
      delete this.jobs[jobName];
      try {
        redisDel(jobName);
      } catch (err) {
        this.logger.error(err);
      }
    }
  }

  /**
   * Runs the given job, if it exists, immediately, without affecting the schedule.
   *
   * @param  {string} jobName The name of the job to be executed
   */
  runJob(jobName) {
    if (this.jobs[jobName]) {
      this.jobs[jobName].action();
    }
  }
}

module.exports = RedCron;
