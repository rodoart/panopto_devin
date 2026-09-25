# PANOPTO — atajos de desarrollo.
#
#   make test          suite regular (ambiente actual: conda/cluster)
#   make local-env     crea el virtualenv local (.venv-local) con pyspark
#   make test-local    suite dentro del venv local (PANOPTO_TEST_MODE=local)
#   make demo-local    ejemplo mínimo sobre samples/sources en Spark local
#   make clean-local   elimina el virtualenv local
#
# Todas las variables de scripts/local_common.sh aplican, p.ej.:
#   PANOPTO_LOCAL_VENV=/tmp/venv PANOPTO_LOCAL_PYTHON=python3.10 make local-env

.PHONY: test local-env test-local demo-local clean-local

test:
	python3 -m pytest tests/ -q

local-env:
	scripts/setup_local_env.sh

test-local:
	scripts/run_local_tests.sh

demo-local:
	scripts/run_local_demo.sh

clean-local:
	rm -rf "$${PANOPTO_LOCAL_VENV:-.venv-local}"
