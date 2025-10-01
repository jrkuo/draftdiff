import gzip
import json

import pytest

from draftdiff.hero_builds import parse_match_heroes
from draftdiff.models.opendota import MatchResponse


@pytest.mark.asyncio
async def test_parse_match_heroes():
    with gzip.open('tests/opendota/match.json.gz', 'r') as rf:
        match_data: dict = json.load(rf)  # type: ignore
        match_response: MatchResponse = MatchResponse(**match_data)  # type: ignore
        match_heroes = await parse_match_heroes(match_response)
        assert len(match_heroes) == 10
