# spam-stream-generator

This is a very basic example of generating synthetic data and publishing it to a Kafka topic.  Deploy it to OpenShift with the following command-line (assuming `myproject` is your OpenShift project and `mybroker` is the hostname of your Kafka broker):

```
oc new-app -n myproject \
   centos/python-36-centos7~https://github.com/willb/spam-stream-generator \
   -e KAFKA_BROKERS=mybroker:9092 \
   -e KAFKA_TOPIC=social-firehose \
   -e RATE=300 \
   -e SPAM_PROPORTION=.95 \
   --name=spam-emitter
```
