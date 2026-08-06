import litellm
import logging
import sys

from dataclasses import dataclass
from typing import List, Optional

from llm_providers.models.llm_model import LLM_Model

# -------------------------------- Logging --------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)
# -------------------------------------------------------------------------


@dataclass
class ScrapingResult:
    content: str
    errors: List[dict]
    urls_scraped: int
    urls_failed: int
    urls_discovered: int
    total_time_taken: float  # in seconds
    timed_out: bool
    llm_model: LLM_Model
    final_landing_etld1: str

    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0

    @property
    def success_rate(self) -> float:
        return ScrapingResult.get_success_rate(self.urls_scraped, self.urls_failed)

    def is_valid(self) -> bool:
        return ScrapingResult.is_scrape_valid(
            self.content,
            self.urls_scraped,
            self.urls_failed,
            self.llm_model,
            self.timed_out,
        )

    @property
    def num_tokens(self) -> int:
        return litellm.token_counter(model=self.llm_model.name, text=self.content)

    def __str__(self) -> str:
        timeout_info = " (TIMED OUT)" if self.timed_out else ""
        return (
            f"ScrapingResult(success_rate={self.success_rate:.2%}, "
            f"urls_scraped={self.urls_scraped}, urls_failed={self.urls_failed}, "
            f"urls_discovered={self.urls_discovered}, "
            f"num_tokens={self.num_tokens}, "
            f"time={self.total_time_taken:.1f}s, "
            f"errors_count={len(self.errors)}{timeout_info})"
        )

    @staticmethod
    def get_success_rate(urls_scraped: int, urls_failed: int) -> float:
        success_rate = (
            urls_scraped / (urls_scraped + urls_failed)
            if (urls_scraped + urls_failed) > 0
            else 0
        )
        return success_rate

    @classmethod
    def is_scrape_valid(
        cls,
        content: str,
        urls_scraped: int,
        urls_failed: int,
        llm_model: LLM_Model,
        timed_out: bool = False,
    ) -> bool:
        num_tokens = litellm.token_counter(model=llm_model.name, text=content)
        success_rate = cls.get_success_rate(urls_scraped, urls_failed)
        return 30 < num_tokens and success_rate > 0.8 and not timed_out

    def print_stats(self) -> None:
        logger.info(f"URLs scraped: {self.urls_scraped}")
        logger.info(f"URLs failed: {self.urls_failed}")
        logger.info(f"URLs discovered: {self.urls_discovered}")
        logger.info(f"Success rate: {self.success_rate:.1%}")
        logger.info(f"Total time taken: {self.total_time_taken:.1f}s")
        if self.timed_out:
            logger.info("Scraping operation TIMED OUT")
        if self.errors:
            logger.info(f"Errors Count: {len(self.errors)}")
            for error in self.errors:
                logger.info(
                    f"- {error['url']}: {error['error']} ({error['error_type']}) at depth {error['depth']}"
                )
