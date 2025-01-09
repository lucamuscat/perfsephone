import json
from pathlib import Path
from types import ModuleType
from typing import List

import pytest
from pyinstrument.frame import Frame

from perfsephone.perfetto_renderer import (
    BLACKLISTED_MODULES,
    RootFrameCannotBeHoistedException,
    hoist,
)


def test_when_hoist__then_grandchild_frame_becomes_parent_sibling() -> None:
    root_frame: Frame = Frame(identifier_or_frame_info="root")
    grandparent_frame: Frame = Frame(identifier_or_frame_info="grandparent")
    parent_to_be_hoisted: Frame = Frame(identifier_or_frame_info="hoist me")
    grandchildren: List[Frame] = [
        Frame(identifier_or_frame_info="grandchild1"),
        Frame(identifier_or_frame_info="grandchild2"),
    ]

    parent_to_be_hoisted.add_children(grandchildren)
    grandparent_frame.add_child(parent_to_be_hoisted)
    root_frame.add_child(grandparent_frame)

    hoist(parent_to_be_hoisted)

    assert list(grandparent_frame.children) == grandchildren


def test_given_root__when_hoist__then_raise_exception() -> None:
    root_frame = Frame(identifier_or_frame_info="root")
    children = [Frame(identifier_or_frame_info="1"), Frame(identifier_or_frame_info="2")]
    root_frame.add_children(children)

    with pytest.raises(RootFrameCannotBeHoistedException):
        hoist(root_frame)


def test_given_no_grand_children__when_hoist__then_remove_frame() -> None:
    root_frame = Frame(identifier_or_frame_info="root")
    frame_to_hoist = Frame(identifier_or_frame_info="hoist me")
    root_frame.add_child(frame_to_hoist)
    hoist(frame_to_hoist)

    assert len(root_frame.children) == 0


def test_given_siblings__when_hoist__then_hoisted_children_are_correctly_ordered() -> None:
    root_frame = Frame(identifier_or_frame_info="root")
    grandparent = Frame(identifier_or_frame_info="grandparent")
    parents = [
        Frame(identifier_or_frame_info="parent1"),
        Frame(identifier_or_frame_info="parent2"),
        Frame(identifier_or_frame_info="parent3"),
    ]
    nieces = [Frame(identifier_or_frame_info="powder"), Frame(identifier_or_frame_info="violet")]

    parents[1].add_children(nieces)
    grandparent.add_children(parents)
    root_frame.add_child(grandparent)

    hoist(parents[1])

    # TODO: Merge into a single assertion.
    assert grandparent.children[1].identifier == "powder"
    assert grandparent.children[2].identifier == "violet"
    assert grandparent.children[3].identifier == "parent3"


@pytest.mark.parametrize("blacklisted_module", BLACKLISTED_MODULES)
def test_when_export_trace__then_pytest_stacks_are_not_included(
    pytester: pytest.Pytester, temp_perfetto_file_path: Path, blacklisted_module: ModuleType
) -> None:
    pytester.makepyfile("""
        from time import time
        def test_do_work() -> None:
            start = time()
            while (time() - start) < 0.05:
                print(start**4)
    """)
    result = pytester.runpytest_inprocess(f"--perfetto={temp_perfetto_file_path}")
    result.assert_outcomes(passed=1)

    # In theory, the best way of querying the perfetto trace file is to use the perfetto package.
    # Unfortunately, the initialization of the trace processor provided by the perfetto package is
    # slow, and is prone to throwing exceptions when multiple trace providers are running.
    trace_file = json.load(temp_perfetto_file_path.open("r"))
    for event in trace_file:
        args = event.get("args")
        if args:
            file_arg = args.get("file")
            if file_arg:
                assert f"/{blacklisted_module.__name__}/" not in file_arg
