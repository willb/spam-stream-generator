import argparse
import logging
import os
import time

must_dry_run = False
try:
    from kafka import KafkaProducer
except Exception:
    must_dry_run = True

import markovify

import gzip

import json

import numpy

from markovify import Text


def load_model(gzfn):
    """ loads a serialized JSON model """
    model = None
    try:
        with gzip.open(gzfn, "rt", encoding="utf-8") as f:
            model: Text = markovify.Text.from_json(f.read())

        return model
    except Exception as ex:
        print(ex)
        return None


def make_sentence(model, length=200):
    return model.make_short_sentence(length)


model_counts = None

def update_generator(models, weights=None):

    if weights is None:
        weights = [1] * len(models)

    choices = []

    total_weight = float(sum(weights))

    for i in range(len(weights)):
        choices.append((float(sum(weights[0:i + 1])) / total_weight, models[i]))

    def choose_model():
        idx = 0
        r = numpy.random.uniform()
        for (p, m) in choices:
            if r <= p:
                return (idx, m)
            idx = idx + 1
        return (idx, choices[-1][1])

    while True:
        idx, model = choose_model()
        tweet = make_sentence(model)

        yield (idx, tweet)


def main(args):
    logging.info('brokers={}'.format(args.brokers))
    logging.info('topic={}'.format(args.topic))
    logging.info('rate={}'.format(args.rate))
    logging.info('source={}'.format(args.source))

    ready = False
    
    while not ready:
        try:
            if args.dry_run:
                logging.info('running in dry run mode; not creating a kafka producer ')
                producer = None
            else:
                logging.info('trying to create a kafka producer at ' + time.asctime() + "...")
                producer = KafkaProducer(bootstrap_servers=args.brokers)
            ready = True
        finally:
            pass

    import time

    logging.info('creating Markov chains from %s, %s at %s' % (args.legitimate_model, args.spam_model, time.asctime()))

    legitimate_model = load_model(args.legitimate_model)
    logging.info('loaded legitimate model')
    spam_model = load_model(args.spam_model)
    logging.info('loaded spam model')

    logging.info('creating update generator ' + time.asctime())

    ug = update_generator([legitimate_model, spam_model], [100 - int(args.spam_proportion * 100), int(args.spam_proportion * 100)])

    logging.info('sending lines ' + time.asctime())
    model_counts = [0, 0]
    msg_count = 0

    while True:
        idx, tweet = next(ug)
        model_counts[idx] += 1
        update = {"text": tweet}
        msg_count += 1

        if msg_count % 100 == 0:
            info =  (model_counts[0], model_counts[1], float(model_counts[1]) / sum(model_counts))
            logging.info("generated %d legitimate messages and %d spam messages; spam fraction is %f" % info)

        if args.dry_run:
            print(json.dumps(update))
        else:
            producer.send(args.topic, bytes(json.dumps(update), "utf-8"))

        time.sleep(1.0 / float(args.rate))


def get_arg(env, default):
    return os.getenv(env) if os.getenv(env, '') is not '' else default


def parse_args(parser):
    args = parser.parse_args()
    args.brokers = get_arg('KAFKA_BROKERS', args.brokers)
    args.topic = get_arg('KAFKA_TOPIC', args.topic)
    args.spam_proportion = float(get_arg('SPAM_PROPORTION', args.spam_proportion))
    args.rate = int(get_arg('RATE', args.rate))
    args.source = get_arg('SOURCE_URI', args.source)
    args.legitimate_model = get_arg('LEGITIMATE_MODEL', args.legitimate_model)
    args.spam_model = get_arg('SPAM_MODEL', args.spam_model)
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info('starting update-generator')
    parser = argparse.ArgumentParser(description='emit synthetic social media updates on kafka')
    parser.add_argument('--dry-run', help='print things, don\'t send them to Kafka', action='store_const', const=True, default=must_dry_run)
    parser.add_argument(
        '--brokers',
        help='The bootstrap servers, env variable KAFKA_BROKERS',
        default='localhost:9092')
    parser.add_argument(
        '--topic',
        help='Topic to publish to, env variable KAFKA_TOPIC',
        default='social-firehose')
    parser.add_argument(
        '--rate',
        type=int,
        help='Lines per second, env variable RATE',
        default=10)
    parser.add_argument(
        '--spam-proportion',
        type=float,
        help='Fraction of messages that are spam, env variable SPAM_PROPORTION',
        default=0.95)
    parser.add_argument(
        '--source',
        help='The source URI for data to emit, env variable SOURCE_URI')
    parser.add_argument(
        '--spam-model',
        help='the gzipped spam model file',
        default='spam_model.json.gz'
    )
    parser.add_argument(
        '--legitimate-model',
        help='the gzipped legitimate model file',
        default='legitimate_model.json.gz'
    )
    args = parse_args(parser)
    main(args)
    logging.info('exiting')