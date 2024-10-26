import time
from typing import Annotated

import polars as pl
import typer

from probe import ask

app = typer.Typer()


def stream_answer(text: str | None):
    if text:
        for word in text.split():
            print(word, end=" ", flush=True)
            time.sleep(0.05)
        print()


@app.command()
def main(
    data: Annotated[str, typer.Argument(help="Path to CSV data file")],
    query: Annotated[str, typer.Argument(help="Query to execute")],
    print_code: bool = typer.Option(
        False, "--print-code", help="Print generated code"
    ),
    print_output: bool = typer.Option(
        False, "--print-output", help="Print raw output"
    ),
    retries: int = typer.Option(0, "--retries", help="Max retries"),
):
    df = pl.scan_csv(data)
    out = ask(
        df,
        query,
        print_code=print_code,
        print_output=print_output,
        max_retries=retries,
        print_answer=False,
    )
    stream_answer(out.answer)


if __name__ == "__main__":
    app()
