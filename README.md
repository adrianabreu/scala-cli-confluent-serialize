I needed a simple program to generate random records on my Kafka set up. As I wanted to make it simple and efficient, this project is built uponthe [Scala CLI](https://scala-cli.virtuslab.org/) and it includes a docker compose image based on [confluent docker images](https://docs.confluent.io/platform/current/installation/docker/image-reference.html).

## How to use?

Type `docker compose up` and you will have a functional kafka cluster with an associated schema registry. Generate a topic and its corresponding schema, then adjust the urls in the project config. 

![Image describing the UI for creating a topic](./docs/images/create_topic.png)

![Image describing where the set schema button is located](./docs/images/set_schema.png)

Type `scala-cli run Producer.sc` and you are set to enjoy your messages!

Feel free to tweak the existing case class and the `GenericRecord` for sending different messages.