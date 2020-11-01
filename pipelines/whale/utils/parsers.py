import re
from whale.utils.markdown_delimiters import (
    COLUMN_DETAILS_DELIMITER,
    DEFINED_METRICS_DELIMITER,
    METRICS_DELIMITER,
    BLOCK_END_DELIMITER,
    PARTITIONS_DELIMITER,
    USAGE_DELIMITER,
    UGC_DELIMITER,
)

HEADER_SECTION = "header"
COLUMN_DETAILS_SECTION = "column_details"
PARTITION_SECTION = "partition"
USAGE_SECTION = "usage"
UGC_SECTION = "ugc"
DEFINED_METRICS_SECTION = "defined_metrics"
METRICS_SECTION = "metrics"
NOTES_SECTION = "notes"


def parse_programmatic_blob(programmatic_blob):

    regex_to_match = (
        "("
        + COLUMN_DETAILS_DELIMITER
        + "|"
        + PARTITIONS_DELIMITER
        + "|"
        + USAGE_DELIMITER
        + "|"
        + METRICS_DELIMITER
        + ")"
    )

    splits = re.split(regex_to_match, programmatic_blob)

    state = HEADER_SECTION
    sections = {
        HEADER_SECTION: [],
        COLUMN_DETAILS_SECTION: [],
        PARTITION_SECTION: [],
        USAGE_SECTION: [],
        METRICS_SECTION: [],
    }

    for clause in splits:
        if clause == COLUMN_DETAILS_DELIMITER:
            state = COLUMN_DETAILS_SECTION
        elif clause == PARTITIONS_DELIMITER:
            state = PARTITION_SECTION
        elif clause == USAGE_DELIMITER:
            state = USAGE_SECTION
        elif clause == METRICS_DELIMITER:
            state = METRICS_SECTION

        sections[state].append(clause)

    for state, clauses in sections.items():
        sections[state] = "".join(clauses)
    return sections


def parse_ugc(ugc_blob):
    regex_to_match = (
        "(" + DEFINED_METRICS_DELIMITER + "|" + BLOCK_END_DELIMITER + ")"
    )  # To reduce priority, END_DELIMITERS always go last
    splits = re.split(regex_to_match, ugc_blob)

    state = NOTES_SECTION
    sections = {
        DEFINED_METRICS_SECTION: [],
        NOTES_SECTION: [],
    }

    for clause in splits:
        if clause == DEFINED_METRICS_DELIMITER:
            state = DEFINED_METRICS_SECTION
        elif clause == BLOCK_END_DELIMITER:
            state = NOTES_SECTION

        if clause not in [DEFINED_METRICS_DELIMITER]:
            sections[state].append(clause)

    return sections


def sections_from_markdown(file_path):

    with open(file_path, "r") as f:
        old_file_text = "".join(f.readlines())

    file_strings = old_file_text.split(UGC_DELIMITER)

    programmatic_blob = file_strings[0]

    programmatic_sections = parse_programmatic_blob(programmatic_blob)

    ugc = "".join(file_strings[1:])

    sections = {
        UGC_SECTION: ugc,
    }
    sections.update(programmatic_sections)
    return sections


def markdown_from_sections(sections: dict):
    programmatic_blob = (
        sections[HEADER_SECTION]
        + sections[COLUMN_DETAILS_SECTION]
        + sections[PARTITION_SECTION]
        + sections[USAGE_SECTION]
        + sections[METRICS_SECTION]
    )

    ugc_blob = sections[UGC_SECTION]
    final_blob = UGC_DELIMITER.join([programmatic_blob, ugc_blob])
    return final_blob
