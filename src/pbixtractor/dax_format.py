"""Offline DAX formatter in daxformatter.com's "long line" style (no web service).

    format_dax("calculate(sum(Sales[Amount]),all(Dates),Dates[Year]=2024)")
    # CALCULATE ( SUM ( Sales[Amount] ), ALL ( Dates ), Dates[Year] = 2024 ) - unless too long
    # or with more than one nested call: then one argument per line, 4-space indent

Rules: a space inside parentheses and after commas, spaces around binary operators, a call on
one line when it fits in LINE_LENGTH characters and holds at most one nested call (else one
argument per line and the ")" on its own line), VAR and RETURN each on their own line with the
RETURN expression one indent deeper, functions and keywords upper case. Names, strings, numbers
and comments stay as written; comments keep their place. Close to daxformatter.com, not
byte-identical. DAX that cannot be parsed - or whose formatted tokens would differ from the
input's - is returned unchanged (logged at DEBUG only).
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Optional, Union

from .dax_parser import (
    BRACKET_NAME,
    COMMENT,
    IDENTIFIER,
    NUMBER,
    OPERATOR,
    PUNCTUATION,
    QUOTED_NAME,
    STRING,
    UNKNOWN,
    WHITESPACE,
    Token,
    tokenize,
)

logger = logging.getLogger("pbixtractor")

LINE_LENGTH = 120
INDENT = 4

# Bare words written in upper case (functions are recognised by their "(")
KEYWORDS = {"VAR", "RETURN", "IN", "NOT", "TRUE", "FALSE", "ASC", "DESC"}

_CLOSED = {
    STRING: re.compile(r'"(?:[^"]|"")*"', re.DOTALL),
    QUOTED_NAME: re.compile(r"'(?:[^']|'')*'", re.DOTALL),
    BRACKET_NAME: re.compile(r"\[(?:[^\]]|\]\])*\]", re.DOTALL),
}


class _UnparseableError(Exception):
    """The DAX cannot be formatted safely."""


# ============================================================================
# Syntax tree
# ============================================================================


@dataclass
class _Atom:
    text: str
    operator: bool = False
    unary: bool = False


@dataclass
class _Comment:
    text: str
    before: bool  # a line break (or the start) right before it in the input
    after: bool  # a line break right after it (always for // and -- comments)

    @property
    def line(self) -> bool:
        return not self.text.startswith("/*")


@dataclass
class _Call:
    name: Optional[str]  # None: a ( ) group or a { } table constructor
    open: str
    close: str
    args: list[list] = field(default_factory=list)
    head: list[_Comment] = field(default_factory=list)  # comments on the "(" line
    after_comma: list[list[_Comment]] = field(default_factory=list)  # per arg after the first


@dataclass
class _VarBlock:
    variables: list[tuple[str, list[_Comment], list]]  # (name, comments after "=", value)
    return_comments: list[_Comment]
    result: list


_Item = Union[_Atom, _Comment, _Call, _VarBlock]


@dataclass
class _Entry:
    token: Token
    break_before: bool  # whitespace with a line break (or the start) right before it
    gap_before: bool  # any whitespace right before it


def _entries(tokens: list[Token]) -> list[_Entry]:
    entries: list[_Entry] = []
    break_before, gap_before = True, False
    for token in tokens:
        if token.kind == WHITESPACE:
            break_before = break_before or "\n" in token.text or "\r" in token.text
            gap_before = True
            continue
        if token.kind == UNKNOWN:
            raise _UnparseableError(f"unexpected character {token.text!r}")
        if token.kind in _CLOSED and not _CLOSED[token.kind].fullmatch(token.text):
            raise _UnparseableError(f"unclosed {token.kind} at {token.position}")
        if token.kind == COMMENT and token.text.startswith("/*"):
            if len(token.text) < 4 or not token.text.endswith("*/"):
                raise _UnparseableError(f"unclosed comment at {token.position}")
        entries.append(_Entry(token, break_before, gap_before))
        break_before, gap_before = False, False
    return entries


class _Parser:
    def __init__(self, entries: list[_Entry]):
        self.entries = entries
        self.pos = 0

    def peek(self, offset: int = 0) -> Optional[_Entry]:
        index = self.pos + offset
        return self.entries[index] if index < len(self.entries) else None

    def is_word(self, entry: Optional[_Entry], word: str) -> bool:
        return (
            entry is not None
            and entry.token.kind == IDENTIFIER
            and entry.token.text.upper() == word
        )

    def is_punct(self, entry: Optional[_Entry], chars: str) -> bool:
        return entry is not None and entry.token.kind == PUNCTUATION and entry.token.text in chars

    def comment(self) -> _Comment:
        entry = self.entries[self.pos]
        self.pos += 1
        following = self.peek()
        after = not entry.token.text.startswith("/*") or (
            following is None or following.break_before
        )
        return _Comment(entry.token.text, entry.break_before, after)

    def trailing_comments(self) -> list[_Comment]:
        """Comments on the same line as the token just consumed."""
        comments = []
        while (entry := self.peek()) and entry.token.kind == COMMENT and not entry.break_before:
            comments.append(self.comment())
        return comments

    def parse(self) -> list:
        items = self.expression(stop_on_keywords=False)
        if self.pos != len(self.entries):
            raise _UnparseableError(f"unexpected {self.entries[self.pos].token.text!r}")
        return items

    def expression(self, stop_on_keywords: bool) -> list:
        items: list[_Item] = []
        while (entry := self.peek()) is not None:
            token = entry.token
            if self.is_punct(entry, ",)}"):
                break
            if self.is_word(entry, "RETURN"):
                if stop_on_keywords:
                    break
                raise _UnparseableError("RETURN without VAR")
            if self.is_word(entry, "VAR"):
                if stop_on_keywords and any(not isinstance(i, _Comment) for i in items):
                    break
                items.append(self.var_block())
                continue
            if token.kind == COMMENT:
                items.append(self.comment())
                continue
            if self.is_punct(entry, "("):
                items.append(self.call(None, "(", ")"))
                continue
            if self.is_punct(entry, "{"):
                items.append(self.call(None, "{", "}"))
                continue
            following = self.peek(1)
            if token.kind == IDENTIFIER and self.is_punct(following, "("):
                self.pos += 1
                items.append(self.call(token.text.upper(), "(", ")"))
                continue
            self.pos += 1
            if (
                token.kind in (IDENTIFIER, QUOTED_NAME)
                and following is not None
                and following.token.kind == BRACKET_NAME
                and not following.gap_before
            ):
                self.pos += 1
                items.append(_Atom(token.text + following.token.text))
            elif token.kind == OPERATOR:
                previous = next((i for i in reversed(items) if not isinstance(i, _Comment)), None)
                unary = token.text in "+-" and (
                    previous is None or (isinstance(previous, _Atom) and previous.operator)
                )
                items.append(_Atom(token.text, operator=True, unary=unary))
            elif token.kind == IDENTIFIER and token.text.upper() in KEYWORDS:
                items.append(_Atom(token.text.upper()))
            elif token.kind in (IDENTIFIER, QUOTED_NAME, BRACKET_NAME, NUMBER, STRING, PUNCTUATION):
                items.append(_Atom(token.text))
            else:
                raise _UnparseableError(f"unexpected {token.text!r}")
        return items

    def call(self, name: Optional[str], open_: str, close: str) -> _Call:
        self.pos += 1  # the opening bracket
        call = _Call(name, open_, close, head=self.trailing_comments())
        while True:
            argument = self.expression(stop_on_keywords=False)
            entry = self.peek()
            if self.is_punct(entry, ","):
                call.args.append(argument)
                self.pos += 1
                call.after_comma.append(self.trailing_comments())
                continue
            if self.is_punct(entry, close):
                self.pos += 1
                call.args.append(argument)
                break
            raise _UnparseableError(f"missing {close!r}")
        if call.args == [[]]:
            call.args = []
        return call

    def var_block(self) -> _VarBlock:
        variables = []
        while self.is_word(self.peek(), "VAR"):
            self.pos += 1
            name = self.peek()
            if name is None or name.token.kind != IDENTIFIER or name.token.text.upper() in KEYWORDS:
                raise _UnparseableError("VAR without a name")
            equals = self.peek(1)
            if equals is None or equals.token.kind != OPERATOR or equals.token.text != "=":
                raise _UnparseableError("VAR without =")
            self.pos += 2
            comments = self.trailing_comments()
            value = self.expression(stop_on_keywords=True)
            if not any(not isinstance(i, _Comment) for i in value):
                raise _UnparseableError(f"VAR {name.token.text} without a value")
            variables.append((name.token.text, comments, value))
        if not self.is_word(self.peek(), "RETURN"):
            raise _UnparseableError("VAR without RETURN")
        self.pos += 1
        return_comments = self.trailing_comments()
        result = self.expression(stop_on_keywords=True)
        if not any(not isinstance(i, _Comment) for i in result):
            raise _UnparseableError("RETURN without an expression")
        return _VarBlock(variables, return_comments, result)


# ============================================================================
# Layout
# ============================================================================


def _nested_calls(items: list) -> int:
    """Function calls anywhere inside these items (groups and { } are not calls)."""
    count = 0
    for item in items:
        if isinstance(item, _Call):
            count += (item.name is not None) + sum(_nested_calls(a) for a in item.args)
        elif isinstance(item, _VarBlock):
            count += sum(_nested_calls(value) for _, _, value in item.variables)
            count += _nested_calls(item.result)
    return count


def _join(pieces: list[tuple[str, bool]]) -> str:
    """Join (text, is unary operator) pieces with single spaces (none after a unary sign)."""
    text = ""
    for i, (piece, _) in enumerate(pieces):
        if i and not pieces[i - 1][1]:
            text += " "
        text += piece
    return text


def _inline_expression(items: list) -> Optional[str]:
    """The items on one line, or None when they cannot be (comments needing a line break)."""
    pieces = []
    for i, item in enumerate(items):
        if isinstance(item, _Comment):
            if item.line or item.before or (item.after and i < len(items) - 1):
                return None
            pieces.append((item.text, False))
        elif isinstance(item, _Atom):
            pieces.append((item.text, item.unary))
        elif isinstance(item, _Call):
            text = _inline_call(item)
            if text is None:
                return None
            pieces.append((text, False))
        else:
            return None  # VAR blocks always take several lines
    return _join(pieces)


def _inline_call(call: _Call) -> Optional[str]:
    comments = call.head + [c for group in call.after_comma for c in group]
    if any(c.line for c in comments):
        return None
    start = f"{call.name} {call.open}" if call.name else call.open
    if not call.args:
        return _join([(start, False), *((c.text, False) for c in call.head), (call.close, False)])
    arguments = []
    for i, argument in enumerate(call.args):
        text = _inline_expression(argument)
        if text is None:
            return None
        before = call.after_comma[i - 1] if i else call.head
        arguments.append(
            _join([*((c.text, False) for c in before), (text, False)]) if before else text
        )
    return f"{start} {', '.join(arguments)} {call.close}"


class _Output:
    def __init__(self):
        self.lines: list[str] = []
        self.current = ""
        self.break_indent: Optional[int] = None  # set after a line comment: break before more

    @property
    def column(self) -> int:
        return len(self.current)

    @property
    def at_line_start(self) -> bool:
        return not self.current.strip()

    def newline(self, indent: int) -> None:
        if not self.at_line_start:
            self.lines.append(self.current)
        self.current = " " * indent
        self.break_indent = None

    def write(self, text: str) -> None:
        if self.break_indent is not None:
            self.newline(self.break_indent)
        first, *rest = text.split("\n")
        self.current += first
        for line in rest:  # a multi-line /* */ comment or string: kept exactly
            self.lines.append(self.current)
            self.current = line

    def comment(self, comment: _Comment, indent: int) -> None:
        self.write(comment.text.rstrip() if comment.line else comment.text)
        if comment.line:
            self.break_indent = indent

    def text(self) -> str:
        lines = [*self.lines, self.current]
        return "\n".join(line.rstrip() for line in lines).strip("\n")


def _render_expression(out: _Output, items: list, indent: int) -> None:
    for i, item in enumerate(items):
        if i:
            previous = items[i - 1]
            if isinstance(previous, _Comment) and previous.after:
                out.newline(indent)
            elif isinstance(item, _Comment) and item.before:
                out.newline(indent)
            elif isinstance(item, _VarBlock):
                out.newline(indent)
            elif not (isinstance(previous, _Atom) and previous.unary):
                out.write(" ")
        elif isinstance(item, _Comment) and item.before and not out.at_line_start:
            out.newline(indent)
        if isinstance(item, _Atom):
            out.write(item.text)
        elif isinstance(item, _Comment):
            out.comment(item, indent)
        elif isinstance(item, _Call):
            _render_call(out, item, indent)
        else:
            if not out.at_line_start:
                out.newline(indent)
            _render_var_block(out, item, indent)


def _render_call(out: _Output, call: _Call, indent: int) -> None:
    inline = _inline_call(call)
    if (
        inline is not None
        and sum(_nested_calls(argument) for argument in call.args) <= 1
        and out.column + len(inline) <= LINE_LENGTH
    ):
        out.write(inline)
        return
    out.write(f"{call.name} {call.open}" if call.name else call.open)
    for comment in call.head:
        out.write(" ")
        out.comment(comment, indent + INDENT)
    for i, argument in enumerate(call.args):
        out.newline(indent + INDENT)
        _render_expression(out, argument, indent + INDENT)
        if i < len(call.args) - 1:
            out.write(",")
            for comment in call.after_comma[i]:
                out.write(" ")
                out.comment(comment, indent + INDENT)
    out.newline(indent)
    out.write(call.close)


def _render_var_block(out: _Output, block: _VarBlock, indent: int) -> None:
    deeper = indent + INDENT
    for name, comments, value in block.variables:
        # Own-line comments ending the value go before the next VAR / RETURN, at its indent
        tail = len(value)
        while tail and isinstance(value[tail - 1], _Comment) and value[tail - 1].before:
            tail -= 1
        value, own_lines = value[:tail], value[tail:]
        if not out.at_line_start:
            out.newline(indent)
        out.write(f"VAR {name} =")
        for comment in comments:
            out.write(" ")
            out.comment(comment, deeper)
        trial = _Output()
        trial.current = "." * (out.column + 1)  # not blank: not a line start
        trial.break_indent = out.break_indent
        _render_expression(trial, value, deeper)
        if out.break_indent is None and not trial.lines:  # a // comment may end the line
            out.write(" ")
        else:
            out.newline(deeper)
        _render_expression(out, value, deeper)
        for comment in own_lines:
            out.newline(indent)
            out.comment(comment, indent)
    out.newline(indent)
    out.write("RETURN")
    for comment in block.return_comments:
        out.write(" ")
        out.comment(comment, deeper)
    out.newline(deeper)
    _render_expression(out, block.result, deeper)


# ============================================================================
# Entry point
# ============================================================================


def _signature(dax: str) -> list[tuple[str, str]]:
    """Tokens that formatting must keep: no whitespace; functions and keywords any case."""
    tokens = [t for t in tokenize(dax) if t.kind != WHITESPACE]
    significant = [t for t in tokens if t.kind != COMMENT]
    following = {
        id(token): significant[i + 1] if i + 1 < len(significant) else None
        for i, token in enumerate(significant)
    }
    signature = []
    for token in tokens:
        text = token.text
        if token.kind == IDENTIFIER:
            after = following[id(token)]
            if text.upper() in KEYWORDS or (after is not None and after.text == "("):
                text = text.upper()
        elif token.kind == COMMENT:
            text = "\n".join(line.rstrip() for line in text.split("\n")).rstrip()
        signature.append((token.kind, text))
    return signature


def format_dax(dax: str) -> str:
    """
    Format a DAX expression in daxformatter.com's "long line" style, offline.

    Args:
        dax: DAX expression as written in the model

    Returns:
        The formatted expression; the input unchanged when it cannot be parsed or formatting
        would change its tokens (logged at DEBUG, never raised)
    """
    if not isinstance(dax, str) or not dax.strip():
        return dax
    try:
        items = _Parser(_entries(tokenize(dax))).parse()
        out = _Output()
        _render_expression(out, items, 0)
        formatted = out.text()
        if _signature(formatted) != _signature(dax):
            raise _UnparseableError("the formatted tokens differ from the input")
        return formatted
    except Exception as e:  # noqa: BLE001 - formatting must never fail a run
        logger.debug(f"DAX left unformatted ({e}): {dax[:80]!r}")
        return dax
