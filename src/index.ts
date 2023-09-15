import NDK, { type NDKRelay } from '@nostr-dev-kit/ndk';
import app from './app';
import path from 'path';
import { Debugger } from 'debug';
import express from 'express';
import * as middlewares from './lib/middlewares';
import { PrismaClient } from '@prisma/client';
import { setUpRoutes, setUpSubscriptions } from '@lib/utils';
import { OutboxService } from '@services/outbox/Outbox';
import { ExtendedRequest } from '@type/request';
import 'websocket-polyfill';

import { logger } from './lib/utils';

const port = process.env.PORT || 8000;

const log: Debugger = logger.extend('index');

// Instantiate prisma client
log('Instantiate prisma');
const prisma = new PrismaClient({
  log: [{ level: 'query', emit: 'event' }],
});

// Instantiate ndk
log('Instantiate NDK');
const ndk = new NDK({
  explicitRelayUrls: process.env.NOSTR_RELAYS?.split(','),
});

log('Subscribing...');
const subscribed = setUpSubscriptions(ndk, path.join(__dirname, 'nostr'));

if (null === subscribed) {
  throw new Error('Error setting up subscriptions');
}

ndk.pool.on('relay:connect', (relay: NDKRelay) => {
  log('Connected to Relay', relay.url);
});

ndk.on('error', (err) => {
  log('Error connecting to Relay', err);
});

// Generate routes
log('Setting up routes...');
const routes = setUpRoutes(express.Router(), path.join(__dirname, 'rest'));

if (null === routes) {
  throw new Error('Error setting up routes');
}

// Setup context
routes.use((req, res, next) => {
  (req as ExtendedRequest).context = {
    prisma,
    outbox: new OutboxService(),
  };
  next();
});

// Setup express routes
app.use('/', routes);

// Setup express routes
app.use(middlewares.notFound);
app.use(middlewares.errorHandler);

//-- Start process --//

// Start listening
app.listen(port, () => {
  log(`Server is running on port ${port}`);
});

// Connect to Nostr
log('Connecting to Nostr...');
ndk.connect();
