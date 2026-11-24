"""Optional live tool-calling probe. Does nothing billable when no key is set."""

import os
import sys


def main() -> None:
    key = os.environ.get("OPENROUTER_API_KEY", "")
    provider = os.environ.get("LLM_PROVIDER", "mock")
    if provider != "openrouter" or not key:
        print("LLM_PROVIDER=mock (or no OPENROUTER_API_KEY). Live spend ₹0.00.")
        print("Set LLM_PROVIDER=openrouter and OPENROUTER_API_KEY to run one real turn.")
        return
    print("A key is set. Run a single demo message through /demo or /dev/simulate after restarting the worker.")
    print("The global llm_budget row stops the worker at MAX_SPEND_INR (default 500).")
    sys.exit(0)


if __name__ == "__main__":
    main()
