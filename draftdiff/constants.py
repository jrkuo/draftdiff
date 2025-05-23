from datetime import datetime

HERO_ID_DICT = {
    "Alchemist": "73",
    "Axe": "2",
    "Bristleback": "99",
    "Centaur Warrunner": "96",
    "Chaos Knight": "81",
    "Dawnbreaker": "135",
    "Doom": "69",
    "Dragon Knight": "49",
    "Earth Spirit": "107",
    "Earthshaker": "7",
    "Elder Titan": "103",
    "Huskar": "59",
    "Kunkka": "23",
    "Legion Commander": "104",
    "Lifestealer": "54",
    "Mars": "129",
    "Night Stalker": "60",
    "Ogre Magi": "84",
    "Omniknight": "57",
    "Primal Beast": "137",
    "Pudge": "14",
    "Slardar": "28",
    "Spirit Breaker": "71",
    "Sven": "18",
    "Tidehunter": "29",
    "Tiny": "19",
    "Treant Protector": "83",
    "Tusk": "100",
    "Underlord": "108",
    "Undying": "85",
    "Wraith King": "42",
    "Anti-Mage": "1",
    "Arc Warden": "113",
    "Bloodseeker": "4",
    "Bounty Hunter": "62",
    "Clinkz": "56",
    "Drow Ranger": "6",
    "Ember Spirit": "106",
    "Faceless Void": "41",
    "Gyrocopter": "72",
    "Hoodwink": "123",
    "Juggernaut": "8",
    "Kez": "145",
    "Luna": "48",
    "Medusa": "94",
    "Meepo": "82",
    "Monkey King": "114",
    "Morphling": "10",
    "Naga Siren": "89",
    "Phantom Assassin": "44",
    "Phantom Lancer": "12",
    "Razor": "15",
    "Riki": "32",
    "Shadow Fiend": "11",
    "Slark": "93",
    "Sniper": "35",
    "Spectre": "67",
    "Templar Assassin": "46",
    "Terrorblade": "109",
    "Troll Warlord": "95",
    "Ursa": "70",
    "Viper": "47",
    "Weaver": "63",
    "Ancient Apparition": "68",
    "Crystal Maiden": "5",
    "Death Prophet": "43",
    "Disruptor": "87",
    "Enchantress": "58",
    "Grimstroke": "121",
    "Jakiro": "64",
    "Keeper of the Light": "90",
    "Leshrac": "52",
    "Lich": "31",
    "Lina": "25",
    "Lion": "26",
    "Muerta": "138",
    "Nature's Prophet": "53",
    "Necrophos": "36",
    "Oracle": "111",
    "Outworld Destroyer": "76",
    "Puck": "13",
    "Pugna": "45",
    "Queen of Pain": "39",
    "Ringmaster": "131",
    "Rubick": "86",
    "Shadow Demon": "79",
    "Shadow Shaman": "27",
    "Silencer": "75",
    "Skywrath Mage": "101",
    "Storm Spirit": "17",
    "Tinker": "34",
    "Warlock": "37",
    "Witch Doctor": "30",
    "Zeus": "22",
    "Abaddon": "102",
    "Bane": "3",
    "Batrider": "65",
    "Beastmaster": "38",
    "Brewmaster": "78",
    "Broodmother": "61",
    "Chen": "66",
    "Clockwerk": "51",
    "Dark Seer": "55",
    "Dark Willow": "119",
    "Dazzle": "50",
    "Enigma": "33",
    "Invoker": "74",
    "Io": "91",
    "Lone Druid": "80",
    "Lycan": "77",
    "Magnus": "97",
    "Marci": "136",
    "Mirana": "9",
    "Nyx Assassin": "88",
    "Pangolier": "120",
    "Phoenix": "110",
    "Sand King": "16",
    "Snapfire": "128",
    "Techies": "105",
    "Timbersaw": "98",
    "Vengeful Spirit": "20",
    "Venomancer": "40",
    "Visage": "92",
    "Void Spirit": "126",
    "Windranger": "21",
    "Winter Wyvern": "112",
}

ID_HERO_DICT = {v: k for k, v in HERO_ID_DICT.items()}

RANK_ID_DICT = {
    "Herald": 1,
    "Guardian": 2,
    "Crusader": 3,
    "Archon": 4,
    "Legend": 5,
    "Ancient": 6,
    "Divine": 7,
    "Immortal": 8,
}

ID_RANK_DICT = {v: k for k, v in RANK_ID_DICT.items()}

RANK_ENUM_ID_DICT = {
    "Herald_Guardian": "1_2",
    "Crusader_Archon": "3_4",
    "Legend_Ancient": "5_6",
    "Divine_Immortal": "7_8",
}
