"""Deterministic code for the mention-stage evaluation.

Nothing here dispatches an LLM request or mutates pipeline state: these modules
read dumps, golden labels and (optionally) pulled wire payloads, and compute.
"""
