"""Módulo de utilidades registry."""

from panopto.metrics.base import MetricRegistry

import panopto.metrics.quality
import panopto.metrics.stability
import panopto.metrics.score
import panopto.metrics.conjugate
import panopto.metrics.target

__all__ = ["MetricRegistry"]
