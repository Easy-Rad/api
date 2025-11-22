# Original author: https://github.com/Tubo

import logging
import re
from pyparsing import FollowedBy, Keyword, Literal, OneOrMore, Optional, Or
from yaml import CLoader, load
from pathlib import Path

yaml_path = Path(__file__).parent / "classifications.yaml"

with open(yaml_path, 'r') as f:
    classifications = load(f, Loader=CLoader)

# Pre-compile the regular expression for better performance
CLEAN_RE = re.compile(r"[^\w\s_]")

def clean(s):
    spaced = CLEAN_RE.sub(" ", s).split()
    return " ".join(spaced)


def to_keywords(key):
    return [Keyword(w) for w in classifications[key]]


def ignore(_):
    return 0


def one(_):
    return 1


def two(_):
    return 2


def three(_):
    return 3


MIDLINES = Or(to_keywords("midline_parts")).setParseAction(one)

SPINE_PARTS = OneOrMore(Or(to_keywords("spine_parts"))).setParseAction(
    lambda t: len(t.asList())
)
SPINE_WHOLE = Literal("WHOLE")("whole").setParseAction(three)
SPINE = (SPINE_PARTS ^ SPINE_WHOLE) + FollowedBy("SPINE")

UNILATERAL = Or(to_keywords("unilateral")).setParseAction(ignore)
BILATERAL = Or(to_keywords("bilateral")).setParseAction(ignore)

DIGIT_NUM = Or(to_keywords("digit_number"))
DIGIT = Or(to_keywords("digit"))
DIGITS = Optional(DIGIT_NUM) + DIGIT

JOINT_NAME = Or(to_keywords("joints"))
JOINT = Literal("JOINT") ^ Literal("JOINTS")
JOINTS = JOINT_NAME + JOINT

PERIPHERAL_SINGULAR = (
    Or(to_keywords("singular_parts")) ^ DIGITS ^ JOINTS
).setParseAction(one)
PERIPHERAL_PLURAL = (
    Or(to_keywords("plural_parts")) ^ DIGITS ^ PERIPHERAL_SINGULAR ^ JOINTS
).setParseAction(two)

PERIPHERIES = (Optional(UNILATERAL) + PERIPHERAL_SINGULAR[1, ...]) ^ (
    Optional(BILATERAL) + PERIPHERAL_PLURAL[1, ...]
)

IGNORED = Or(to_keywords("ignore"))


pattern = MIDLINES ^ SPINE ^ PERIPHERIES
pattern.ignore(IGNORED)


def split(cleaned_text):
    result = pattern.search_string(cleaned_text)
    return result.as_list()

def calculate(list_of_parts):
    return sum(sum(i) for i in list_of_parts)

def parse_cleaned(cleaned_text):
    count = max(1, calculate(split(cleaned_text)))
    logging.debug(f"Calculated body parts as {count} for '{cleaned_text}'")
    return count