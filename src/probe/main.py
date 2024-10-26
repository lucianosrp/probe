from typing import Any

import ell
import polars as pl
from ell.types import Message
from pydantic import BaseModel, Field

from probe import prompts

MODEL = "claude-3-5-sonnet-20241022"


class PythonScript(BaseModel, arbitrary_types_allowed=True):
    user_query: str | None = None
    code_str: str = Field("the python code of the pl.Expr")
    error: str | None = None
    code: pl.Expr | pl.LazyFrame | None = None
    console_output: str | None = None
    answer: str | None = None
    result: Any = None


@ell.complex(
    model=MODEL,
    temperature=0.7,
    max_tokens=1000,
)
def code_creator(df: pl.LazyFrame, query: str):
    return [
        ell.system(
            prompts.SYS_PROMPT.format(df.collect_schema(), df.head(1).collect())
            + f"\n{prompts.POLARS_TWEAKS}"
        ),
        ell.user(query),
    ]


@ell.complex(
    model=MODEL,
    temperature=0.7,
    max_tokens=1000,
)
def check_code(output: PythonScript):
    return [
        ell.system(prompts.CHECK_CODE),
        ell.user(f"""

        Code being checked:
        {output.code}

        Error/Output:
        {output.error}
        """),
    ]


def safe_eval(code: str, data: pl.LazyFrame) -> PythonScript:
    # Create restricted globals
    safe_globals = {
        "pl": pl,
        "df": data,
        "print": print,
        "__builtins__": {
            "True": True,
            "False": False,
            "None": None,
            "abs": abs,
            "float": float,
            "int": int,
            "len": len,
            "max": max,
            "min": min,
            "round": round,
            "sum": sum,
            "str": str,
        },
    }

    output = PythonScript(code_str=code)
    try:
        output.code = eval(code, safe_globals, safe_globals)
    except Exception as e:
        output.error = str(e)
    return output


def execute_with_retry(
    initial_code_result: Message, data: pl.LazyFrame, max_retries=3
) -> PythonScript:
    """
    Executes code with retry attempts if errors occur.

    Args:
        initial_code_result: The initial code execution result
        max_retries (int): Maximum number of retry attempts, defaults to 3

    Returns:
        PythonScript: The final execution result after retries
    """
    execution_result = safe_eval(
        initial_code_result.text.replace("groupby", "group_by"), data
    )

    retry_count = 0
    while execution_result.error and retry_count < max_retries:
        print("RETRYING", execution_result.error, execution_result.code_str)
        checked_code = check_code(execution_result)
        print(checked_code)
        checked_execution = safe_eval(checked_code.text, data)
        print(checked_execution)
        if not checked_execution.error:
            execution_result = checked_execution
            break

        retry_count += 1

    return execution_result


@ell.complex(
    model=MODEL,
    temperature=0.7,
    max_tokens=200,
)
def translate_output(output: PythonScript):
    return [
        ell.system(prompts.TRANSLATE),
        ell.user(
            f"Query: {output.user_query}, console_output:\n{output.console_output}"
        ),
    ]


def ask(
    df: pl.LazyFrame,
    query: str,
    print_query: bool = False,
    print_code: bool = False,
    print_output: bool = False,
    max_retries: int = 0,
    print_answer: bool = True,
):
    output = code_creator(df, query)
    output = execute_with_retry(output, data=df, max_retries=max_retries)
    with pl.Config(tbl_rows=100, fmt_str_lengths=50):
        if isinstance(output.code, pl.Expr):
            result = df.select(output.code).collect()
            output.console_output = str(result)
        else:
            result = output.code.lazy().collect()
            output.console_output = str(result)
    output.user_query = query
    output.answer = translate_output(output).text
    output.result = result

    if print_query:
        print(query, "\n-------")

    if print_answer:
        print(output.answer)

    if print_code:
        print("\n\n-------\n\n" + output.code_str)

    if print_output:
        print("\n\n-------\n\n", output.console_output)
    return output


if __name__ == "__main__":
    df = pl.scan_csv("data/sales_data.csv")
    query = "What is the month-over-month sales growth rate?"
    out = ask(df, query, print_query=True, print_code=True, print_output=True)
