"""What each ADF activity type is and whether its work is visible."""
from __future__ import annotations

import json
import re
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Activity knowledge base (from the field-tested distiller)
# ---------------------------------------------------------------------------

ACTIVITY_CATALOG: Dict[str, Tuple[str, str]] = {
    "Copy":                     ("movement",    "Copies data from a source dataset to a sink dataset"),
    "ExecuteDataFlow":          ("transform",   "Runs a Mapping Data Flow (Spark)"),
    "ExecuteWranglingDataflow": ("transform",   "Runs a Power Query (wrangling) data flow"),
    "TridentNotebook":          ("transform",   "Runs a Microsoft Fabric notebook"),
    "PBISemanticModelRefresh":  ("external",    "Refreshes a Power BI semantic model"),
    "RefreshDataflow":          ("external",    "Refreshes a Power BI / Fabric dataflow"),
    "Office365Outlook":         ("external",    "Sends an Office 365 Outlook email"),
    "DatabricksNotebook":       ("transform",   "Runs a Databricks notebook"),
    "DatabricksSparkPython":    ("transform",   "Runs a Python script on Databricks"),
    "DatabricksSparkJar":       ("transform",   "Runs a JAR on Databricks"),
    "HDInsightHive":            ("transform",   "Runs a Hive script"),
    "HDInsightPig":             ("transform",   "Runs a Pig script"),
    "HDInsightSpark":           ("transform",   "Runs a Spark program"),
    "HDInsightMapReduce":       ("transform",   "Runs a MapReduce job"),
    "HDInsightStreaming":       ("transform",   "Runs a Hadoop streaming job"),
    "SqlServerStoredProcedure": ("transform",   "Executes a stored procedure"),
    "Script":                   ("transform",   "Executes SQL script(s)"),
    "SynapseNotebook":          ("transform",   "Runs a Synapse notebook"),
    "SparkJob":                 ("transform",   "Runs a Synapse Spark job definition"),
    "AzureMLBatchExecution":    ("transform",   "Runs an Azure ML batch job"),
    "AzureMLExecutePipeline":   ("transform",   "Runs an Azure ML pipeline"),
    "AzureFunctionActivity":    ("external",    "Calls an Azure Function"),
    "WebActivity":              ("external",    "Calls a REST endpoint"),
    "WebHook":                  ("external",    "Calls a webhook and waits for callback"),
    "Custom":                   ("external",    "Runs custom code on Azure Batch"),
    "Lookup":                   ("read",        "Reads a value/rowset (config or control data)"),
    "GetMetadata":              ("read",        "Reads dataset metadata (file lists, existence, size)"),
    "Validation":               ("read",        "Waits until a dataset exists / meets criteria"),
    "Delete":                   ("management",  "Deletes files/folders at a dataset location"),
    "ExecutePipeline":          ("orchestration", "Invokes another pipeline"),
    "ExecuteSSISPackage":       ("orchestration", "Runs an SSIS package on an IR"),
    "ForEach":                  ("control",     "Iterates a collection, running inner activities per item"),
    "Until":                    ("control",     "Loops inner activities until an expression is true"),
    "IfCondition":              ("control",     "Branches into true/false activity sets"),
    "Switch":                   ("control",     "Branches into one of several cases"),
    "Filter":                   ("control",     "Filters an array with an expression"),
    "SetVariable":              ("control",     "Sets a pipeline variable"),
    "AppendVariable":           ("control",     "Appends to an array variable"),
    "Wait":                     ("control",     "Waits n seconds"),
    "Fail":                     ("control",     "Deliberately fails the pipeline"),
}

# Activities whose real work happens in code ADF cannot see.
OPAQUE_TYPES = {
    "DatabricksNotebook", "DatabricksSparkPython", "DatabricksSparkJar",
    "SynapseNotebook", "SparkJob", "TridentNotebook", "Custom", "ExecuteSSISPackage",
    "HDInsightHive", "HDInsightPig", "HDInsightSpark", "HDInsightMapReduce",
    "HDInsightStreaming", "AzureMLBatchExecution", "AzureMLExecutePipeline",
}
