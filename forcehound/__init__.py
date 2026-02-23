"""ForceHound — Unified Salesforce BloodHound collector.

Supports two collection backends:
  - API: Privileged REST API collector using simple_salesforce (SOQL queries)
  - Aura: Low-privilege collector using Salesforce Aura/Lightning endpoints

Output format: OpenGraph v1 JSON for BloodHound ingestion.
"""

__version__ = "0.1.0"

from forcehound.collectors.api_collector import APICollector
from forcehound.collectors.aura_collector import AuraCollector
from forcehound.graph.builder import GraphBuilder

__all__ = ["APICollector", "AuraCollector", "GraphBuilder"]
