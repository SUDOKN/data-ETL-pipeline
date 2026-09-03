# Warn / raise / silent-drop inventory

Scope: `apps/data_etl_app/src` + `packages`, tests and `pipeline_v3_evidence` excluded.
Method: AST walk (not grep) over 286 production files. Generated 2026-08-27.

## Census

| Measure | Count |
|---|---|
| `raise` statements | 477 |
| `except` handlers | 251 |
| — re-raise (bare or wrapped) | 60 |
| — log then raise | 38 |
| — log then swallow | 114 |
| — **swallow with no log** | 39 |
| `contextlib.suppress` blocks | 0 |
| warn/error call sites | 263 |
| — followed by a raise | 43 |
| — followed by return/continue/break | 95 |
| — fall through | 125 |

## Every silent handler (swallows with no log)


**apps/data_etl_app/src/data_etl_app/bots/gt_scrape_queue_bot.py**

- `:263` — `except Exception` → `subject = f'GT: Error downloading existing scraped ; if item.email_errand: ; await ScrapingError.ins`
- `:324` — `except Exception` → `subject = f'GT: Error downloading existing scraped ; if item.email_errand: ; await ScrapingError.ins`

**apps/data_etl_app/src/data_etl_app/bots/new_scrape_queue_bot.py**

- `:266` — `except Exception` → `subject = f'Error downloading existing scraped fil ; await ScrapingError.insert_one(ScrapingError(cr`
- `:322` — `except Exception` → `subject = f'Error downloading existing scraped fil ; await ScrapingError.insert_one(ScrapingError(cr`

**apps/data_etl_app/src/data_etl_app/knowledge/ontology/validate_ontology_rdf.py**

- `:91` — `except Exception` → `return False`
- `:265` — `except ValueError` → `validation_result.add_error(f'Failed to get label `
- `:267` — `except Exception` → `validation_result.add_error(f'Unexpected error val`
- `:442` — `except Exception` → `result.add_error(f'Unexpected error validating URI`
- `:455` — `except Exception` → `result.add_error(f'Failed to parse RDF graph: {e}' ; return result`
- `:483` — `except Exception` → `result = ValidationResult() ; result.add_error(f'Failed to read RDF file {file_p ; return result`

**apps/data_etl_app/src/data_etl_app/knowledge/ttlgenerator3_new.py**

- `:291` — `except Exception` → `pass`

**apps/data_etl_app/src/data_etl_app/scripts/generate_ttl_from_mfg.py**

- `:181` — `except ValueError` → `total_processed = skip + batch_size`

**apps/data_etl_app/src/data_etl_app/services/prompt_assembly_service.py**

- `:961` — `except ValueError` → `parent = children = ''`

**packages/core/src/core/models/pipeline_nodes/base/base_llm_recursive_extraction_node.py**

- `:152` — `except Exception` → `embed_error = exc`

**packages/core/src/core/services/pipeline_nodes/multi_stage/llm_phrase_search_node_service.py**

- `:120` — `except ValueError` → `break`

**packages/core/src/core/services/rdf_validation_service.py**

- `:27` — `except Exception` → `return False`
- `:63` — `except Exception` → `issues.append({'type': 'concept_tree_validation_er`

**packages/core/src/core/utils/extraction_dump_util.py**

- `:829` — `except ValueError` → `entry['phrases_note'] = 'response_not_the_search_w ; return `

**packages/core/src/core/utils/str_util.py**

- `:37` — `except json.JSONDecodeError` → `pass`

**packages/llm_providers/src/llm_providers/utils/ask_llm_util.py**

- `:58` — `except (TypeError, ValueError)` → `return None`

**packages/llm_providers/src/llm_providers/utils/open_ai/openai_file_util.py**

- `:237` — `except Exception` → `failed = True ; if first_exception is None: ; break`

**packages/pure_utils/src/pure_utils/url_util.py**

- `:151` — `except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, requests.exceptions.Timeout)` → `return False`
- `:201` — `except requests.RequestException` → `r = s.get(start_url, allow_redirects=True, timeout`

**packages/scraper/src/scraper/services/url_scraper_service.py**

- `:118` — `except Exception` → `pass`
- `:135` — `except StaleElementReferenceException` → `try:`
- `:145` — `except Exception` → `pass`
- `:147` — `except Exception` → `pass`
- `:149` — `except Exception` → `pass`
- `:167` — `except Exception` → `pass`
- `:210` — `except Exception` → `text = ''`
- `:298` — `except Empty` → `if cancel_event.is_set(): ; continue`
- `:459` — `except Empty` → `pass`
- `:497` — `except TimeoutError` → `total_time_taken = time.monotonic() - start_time i ; raw_content = ''.join(results) if 'results' in `
- `:518` — `except Exception` → `total_time_taken = time.monotonic() - start_time i ; return ScrapingResult(content='', errors`

**packages/scraper/src/scraper/utils/selenium/chrome_driver_manager.py**

- `:107` — `except Exception` → `pass`
- `:122` — `except Exception` → `pass`
- `:288` — `except KeyError` → `ch = meta['channels'].get(self.channel) or meta['c`
- `:415` — `except Exception` → `pass`

**packages/scraper/src/scraper/utils/selenium/driver_factory.py**

- `:120` — `except Exception` → `pass`

## Every non-raising warn/error site in the extraction core


**packages/core/src/core/models/pipeline_nodes/base/base_llm_recursive_extraction_node.py**

- `:215` `warning` → `FALLTHROUGH` — logger.warning(f"[{subject.subject_unique_id}] {self.__class__.__name__} ('{self.field_type.name}') held an embedding er

**packages/core/src/core/models/pipeline_nodes/multi_stage/base/llm_phrase_iterative_grounding_node.py**

- `:559` `warning` → `FALLTHROUGH` — logger.warning(f'Child concept {child_concept.name} is not a child of parent concept {parent_concept.name}. Marking as f

**packages/core/src/core/models/pipeline_nodes/multi_stage/concept/concept_reconcile_node.py**

- `:303` `error` → `FALLTHROUGH` — logger.error(f"[{subject.subject_unique_id}] full-run dump could not build the synthesis block of chunk {chunk_bounds} o
- `:321` `error` → `FALLTHROUGH` — logger.error(f"[{subject.subject_unique_id}] full-run dump could not build the fold block of chunk {chunk_bounds} of '{s

**packages/core/src/core/models/pipeline_nodes/multi_stage/keyword/keyword_reconcile_node.py**

- `:244` `error` → `FALLTHROUGH` — logger.error(f"[{subject.subject_unique_id}] full-run dump could not build the synthesis block of chunk {chunk_bounds} o
- `:262` `error` → `FALLTHROUGH` — logger.error(f"[{subject.subject_unique_id}] full-run dump could not build the fold block of chunk {chunk_bounds} of '{s

**packages/core/src/core/services/phrase_blocks_contract.py**

- `:309` `warning` → `FALLTHROUGH` — logger.warning(f'{where}: repaired response phrase {received!r} -> sent phrase {sent!r}')
- `:315` `warning` → `FALLTHROUGH` — logger.warning(f'{where}: dropping {len(reconciliation.extra)} response phrase(s) that were never sent: {reconciliation.
- `:327` `warning` → `FALLTHROUGH` — logger.warning(message)
- `:485` `warning` → `FALLTHROUGH` — logger.warning(message)
- `:611` `warning` → `FALLTHROUGH` — logger.warning(f'{where}: dropping {len(unknown)} response mention id(s) that were never sent: {unknown}')
- `:617` `warning` → `FALLTHROUGH` — logger.warning(f'{where}: response described {len(response_by_mention_id) - len(unknown)} of {len(sent_ids)} sent mentio

**packages/core/src/core/services/pipeline_nodes/multi_stage/llm_grounding_node_service.py**

- `:161` `warning` → `FALLTHROUGH` — logger.warning(f'repaired option casing {label!r} -> {canonical!r} for record {entry.record_id}')
- `:190` `warning` → `FALLTHROUGH` — logger.warning(f'record {entry.record_id}: dropping explanation volunteered beside {len(tags)} tag(s)')
- `:213` `warning` → `FALLTHROUGH` — logger.warning(f'record {entry.record_id}: dropped {len(dropped)} non-vocabulary option(s) {_quoted(dropped)}; {len(tags
- `:395` `warning` → `FALLTHROUGH` — logger.warning(f'{stage_label}: {len(answer.missing_ids)} record(s) of chunk {chunk_bounds} in {subject_unique_id}:{fiel

**packages/core/src/core/services/pipeline_nodes/multi_stage/llm_phrase_mention_collection_node_service.py**

- `:680` `warning` → `CONTINUE` — logger.warning(f'mention_collection: mention id {mention_id!r} answered in two requests of sub-window {sub_bounds} in {s

**packages/core/src/core/services/pipeline_nodes/multi_stage/llm_phrase_search_node_service.py**

- `:136` `warning` → `FALLTHROUGH` — logger.warning(f'parse_llm_search_response: repetitive response{where} — {len(phrases)} phrases, {len(unique)} unique; {
- `:170` `error` → `FALLTHROUGH` — logger.error(f'parse_llm_search_response: truncated response{where} ({len(gpt_response)} chars) — salvaged {len(salvaged

**packages/core/src/core/services/pipeline_nodes/multi_stage/llm_phrase_synthesis_node_service.py**

- `:502` `warning` → `FALLTHROUGH` — logger.warning(f'{where}: dropping {len(unknown_ids)} response record id(s) that were never sent: {unknown_ids}')
- `:509` `warning` → `FALLTHROUGH` — logger.warning(f'{where}: response synthesized {len(held)} of {len(sent_ids)} sent records; nothing came back for {missi
- `:570` `warning` → `CONTINUE` — logger.warning(f'synthesis: record id {record_id!r} answered in two requests of chunk {chunk_bounds} in {subject_unique_

**packages/core/src/core/services/pipeline_nodes/multi_stage/llm_relationship_screening_node_service.py**

- `:326` `warning` → `FALLTHROUGH` — logger.warning(f'record_screening: {len(answer.missing_ids)} record(s) of chunk {chunk_bounds} in {subject_unique_id}:{f

**packages/core/src/core/services/pipeline_nodes/partial_run_dump.py**

- `:137` `error` → `FALLTHROUGH` — logger.error(f"[{subject_unique_id}] partial dump could not parse the single-stage result for '{field_type.name}' chunk 
- `:249` `error` → `FALLTHROUGH` — logger.error(f"[{subject_unique_id}] partial dump could not read the synthesis of chunk {chunk_bounds} of '{field_type.n
- `:301` `error` → `FALLTHROUGH` — logger.error(f"[{subject_unique_id}] partial dump could not fold chunk {chunk_bounds} of '{field_type.name}': {fold_erro
- `:344` `error` → `FALLTHROUGH` — logger.error(f"[{subject_unique_id}] Failed to write partial run dump for '{field_type.name}' stopped at {stopped_at.val

**packages/core/src/core/utils/extraction_dump_util.py**

- `:256` `error` → `FALLTHROUGH` — logger.error(f"extraction dump row inconsistency for record {row.get('record_id')!r}: writer chose status {status!r} but
- `:395` `error` → `FALLTHROUGH` — logger.error(f"extraction dump row inconsistency for group {row.get('group_id')!r}: writer chose status {status!r} but t

**packages/core/src/core/utils/str_util.py**

- `:127` `warning` → `FALLTHROUGH` — logger.warning(f'Failed to fix JSON with quote escaping approach: {e}')
- `:130` `warning` → `FALLTHROUGH` — logger.warning(f'Error while attempting to fix JSON quotes: {e}')
