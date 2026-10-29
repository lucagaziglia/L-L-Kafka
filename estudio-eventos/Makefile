.PHONY: demo test ml marketing kafka-ml kafka-marketing
PYTHON ?= python3
demo:
	$(PYTHON) -m ll_events demo-all
ml:
	$(PYTHON) -m ll_events demo-ml
marketing:
	$(PYTHON) -m ll_events demo-marketing
test:
	$(PYTHON) -m unittest discover -s tests -v
kafka-ml:
	$(PYTHON) scripts/kafka_demo.py --case ml
kafka-marketing:
	$(PYTHON) scripts/kafka_demo.py --case marketing
