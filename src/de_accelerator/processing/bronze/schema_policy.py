"""Bronze schema-evolution policy (doc 03 §4): pure functions over Spark types; no I/O."""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql.types import (
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StructType,
    TimestampType,
)

from de_accelerator.core.errors import SchemaChangeError

_INT_RANK: dict[type, int] = {ByteType: 1, ShortType: 2, IntegerType: 3, LongType: 4}


def is_safe_widening(narrow: DataType, wide: DataType) -> bool:
    """True if every value of `narrow` is representable in `wide` without loss."""
    if narrow == wide:
        return True
    if type(narrow) in _INT_RANK and type(wide) in _INT_RANK:
        return _INT_RANK[type(narrow)] < _INT_RANK[type(wide)]
    if isinstance(narrow, FloatType) and isinstance(wide, DoubleType):
        return True
    if isinstance(narrow, DecimalType) and isinstance(wide, DecimalType):
        return wide.scale >= narrow.scale and (wide.precision - wide.scale) >= (narrow.precision - narrow.scale)
    if type(narrow) in _INT_RANK and isinstance(wide, DecimalType):
        digits = {ByteType: 3, ShortType: 5, IntegerType: 10, LongType: 19}[type(narrow)]  # type: ignore[index]
        return wide.precision - wide.scale >= digits
    if isinstance(narrow, DateType) and isinstance(wide, TimestampType):
        return True
    return False


@dataclass
class SchemaDecision:
    change_type: str = "NONE"  # CREATED | NONE | ADDED | WIDENED | DROPPED | RESCUED (may be combined with '+')
    add_columns: list[str] = field(default_factory=list)
    cast_to_table: dict[str, DataType] = field(default_factory=dict)  # incoming narrower than table: cast up
    widen_table: dict[str, DataType] = field(default_factory=dict)  # incoming wider than table: table type must widen
    rescue: list[str] = field(default_factory=list)  # incompatible columns moved to _rescued_data
    dropped: list[str] = field(default_factory=list)  # in table, absent from this batch (kept; values null)

    @property
    def needs_merge_schema(self) -> bool:
        return bool(self.add_columns)


def decide(incoming: StructType, existing: StructType | None, mode: str) -> SchemaDecision:
    """Compare the extracted (raw) schema with the Bronze table's raw schema under `mode`
    (FAIL | ADD_COLUMNS | ALLOW_WIDENING | RESCUE). Raises SchemaChangeError when the change is not permitted."""
    if existing is None:
        return SchemaDecision("CREATED")
    ex = {f.name.lower(): f for f in existing.fields if not f.name.startswith("_")}
    inc = {f.name.lower(): f for f in incoming.fields}
    d = SchemaDecision()
    parts: list[str] = []
    for key, f in inc.items():
        if key not in ex:
            if mode == "FAIL":
                raise SchemaChangeError(f"new source column '{f.name}' not allowed (schema_evolution_mode=FAIL)", column=f.name)
            d.add_columns.append(f.name)
            continue
        t_ex, t_in = ex[key].dataType, f.dataType
        if t_ex == t_in:
            continue
        if is_safe_widening(t_in, t_ex):  # incoming is narrower than the table: lossless upcast, no change to the table
            d.cast_to_table[ex[key].name] = t_ex
        elif is_safe_widening(t_ex, t_in) and mode == "ALLOW_WIDENING":
            d.widen_table[ex[key].name] = t_in
        elif mode == "RESCUE":
            d.rescue.append(f.name)
        else:
            raise SchemaChangeError(
                f"column '{f.name}' changed type {t_ex.simpleString()} -> {t_in.simpleString()} (mode={mode}); "
                "incompatible change cannot be applied", column=f.name)  # fmt: skip
    d.dropped = [ex[k].name for k in ex if k not in inc]
    if d.add_columns:
        parts.append("ADDED")
    if d.widen_table:
        parts.append("WIDENED")
    if d.rescue:
        parts.append("RESCUED")
    if d.dropped:
        parts.append("DROPPED")
    d.change_type = "+".join(parts) or "NONE"
    return d
