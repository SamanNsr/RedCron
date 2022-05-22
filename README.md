## RedCron

Distributed node.js cronjob with redlock algorithm

## Installation

Run below script and copy the file into yur project

```
npm i lodash node-cron cron-parser redlock winston
```

## How to use

```node
const RedCron = require('./RedCron');

const redCron = new RedCron(redisClient);

redCron.addJob(job, interval, jobName);
```
