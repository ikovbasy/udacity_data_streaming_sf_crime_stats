import producer_server
import json


def run_kafka_server():
	# TODO get the json file path
    input_file = "police-department-calls-for-service.json"

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="udacity.project2.police.calls_3",
        value_serializer=lambda m: json.dumps(m).encode('utf-8'),
        bootstrap_servers="localhost:9092",
        client_id="7"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
