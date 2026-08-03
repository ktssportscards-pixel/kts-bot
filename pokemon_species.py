"""
Pokemon raw-singles buying rules — data + helpers (added Jul 31 2026 weekend).

Buying spec (Kevin, Jul 31): $40-$80 per card, English Near Mint, ungraded,
2022+ sets only, NO trainer cards at all, NO Master Ball versions.

Trainer detection is FAIL-CLOSED: a card is only accepted as a Pokemon
(character) card if its product name contains a recognized species name.
Trainers, items, supporters, energy, and sealed product contain no species
name and are rejected. A handful of trainer/item cards DO contain a species
name ("Rotom Phone") — those are on an explicit block list.

Set-era filtering is a BLOCK LIST of pre-2022 English sets (complete through
the Jan 2026 knowledge cutoff). Unknown set names are assumed NEWER than 2022
and allowed — that way brand-new sets (e.g. "Ascended Heroes") keep working
without a code change. Matching is exact on the normalized name (never
substring), so "Evolutions" (2016, blocked) can NOT hit "Prismatic
Evolutions" (2025, allowed).

SPECIES_SLUGS: all 1025 species, generated from PokeAPI /pokemon-species
on 2026-07-31. Slugs use "-" where card names use spaces/punctuation;
canon() folds both to the same form.
"""

import re
import unicodedata

SPECIES_SLUGS = [
    'bulbasaur', 'ivysaur', 'venusaur', 'charmander', 'charmeleon', 'charizard', 'squirtle',
    'wartortle', 'blastoise', 'caterpie', 'metapod', 'butterfree', 'weedle', 'kakuna', 'beedrill',
    'pidgey', 'pidgeotto', 'pidgeot', 'rattata', 'raticate', 'spearow', 'fearow', 'ekans',
    'arbok', 'pikachu', 'raichu', 'sandshrew', 'sandslash', 'nidoran-f', 'nidorina', 'nidoqueen',
    'nidoran-m', 'nidorino', 'nidoking', 'clefairy', 'clefable', 'vulpix', 'ninetales', 'jigglypuff',
    'wigglytuff', 'zubat', 'golbat', 'oddish', 'gloom', 'vileplume', 'paras', 'parasect', 'venonat',
    'venomoth', 'diglett', 'dugtrio', 'meowth', 'persian', 'psyduck', 'golduck', 'mankey',
    'primeape', 'growlithe', 'arcanine', 'poliwag', 'poliwhirl', 'poliwrath', 'abra', 'kadabra',
    'alakazam', 'machop', 'machoke', 'machamp', 'bellsprout', 'weepinbell', 'victreebel', 'tentacool',
    'tentacruel', 'geodude', 'graveler', 'golem', 'ponyta', 'rapidash', 'slowpoke', 'slowbro',
    'magnemite', 'magneton', 'farfetchd', 'doduo', 'dodrio', 'seel', 'dewgong', 'grimer', 'muk',
    'shellder', 'cloyster', 'gastly', 'haunter', 'gengar', 'onix', 'drowzee', 'hypno', 'krabby',
    'kingler', 'voltorb', 'electrode', 'exeggcute', 'exeggutor', 'cubone', 'marowak', 'hitmonlee',
    'hitmonchan', 'lickitung', 'koffing', 'weezing', 'rhyhorn', 'rhydon', 'chansey', 'tangela',
    'kangaskhan', 'horsea', 'seadra', 'goldeen', 'seaking', 'staryu', 'starmie', 'mr-mime',
    'scyther', 'jynx', 'electabuzz', 'magmar', 'pinsir', 'tauros', 'magikarp', 'gyarados',
    'lapras', 'ditto', 'eevee', 'vaporeon', 'jolteon', 'flareon', 'porygon', 'omanyte', 'omastar',
    'kabuto', 'kabutops', 'aerodactyl', 'snorlax', 'articuno', 'zapdos', 'moltres', 'dratini',
    'dragonair', 'dragonite', 'mewtwo', 'mew', 'chikorita', 'bayleef', 'meganium', 'cyndaquil',
    'quilava', 'typhlosion', 'totodile', 'croconaw', 'feraligatr', 'sentret', 'furret', 'hoothoot',
    'noctowl', 'ledyba', 'ledian', 'spinarak', 'ariados', 'crobat', 'chinchou', 'lanturn',
    'pichu', 'cleffa', 'igglybuff', 'togepi', 'togetic', 'natu', 'xatu', 'mareep', 'flaaffy',
    'ampharos', 'bellossom', 'marill', 'azumarill', 'sudowoodo', 'politoed', 'hoppip', 'skiploom',
    'jumpluff', 'aipom', 'sunkern', 'sunflora', 'yanma', 'wooper', 'quagsire', 'espeon', 'umbreon',
    'murkrow', 'slowking', 'misdreavus', 'unown', 'wobbuffet', 'girafarig', 'pineco', 'forretress',
    'dunsparce', 'gligar', 'steelix', 'snubbull', 'granbull', 'qwilfish', 'scizor', 'shuckle',
    'heracross', 'sneasel', 'teddiursa', 'ursaring', 'slugma', 'magcargo', 'swinub', 'piloswine',
    'corsola', 'remoraid', 'octillery', 'delibird', 'mantine', 'skarmory', 'houndour', 'houndoom',
    'kingdra', 'phanpy', 'donphan', 'porygon2', 'stantler', 'smeargle', 'tyrogue', 'hitmontop',
    'smoochum', 'elekid', 'magby', 'miltank', 'blissey', 'raikou', 'entei', 'suicune', 'larvitar',
    'pupitar', 'tyranitar', 'lugia', 'ho-oh', 'celebi', 'treecko', 'grovyle', 'sceptile', 'torchic',
    'combusken', 'blaziken', 'mudkip', 'marshtomp', 'swampert', 'poochyena', 'mightyena', 'zigzagoon',
    'linoone', 'wurmple', 'silcoon', 'beautifly', 'cascoon', 'dustox', 'lotad', 'lombre', 'ludicolo',
    'seedot', 'nuzleaf', 'shiftry', 'taillow', 'swellow', 'wingull', 'pelipper', 'ralts', 'kirlia',
    'gardevoir', 'surskit', 'masquerain', 'shroomish', 'breloom', 'slakoth', 'vigoroth', 'slaking',
    'nincada', 'ninjask', 'shedinja', 'whismur', 'loudred', 'exploud', 'makuhita', 'hariyama',
    'azurill', 'nosepass', 'skitty', 'delcatty', 'sableye', 'mawile', 'aron', 'lairon', 'aggron',
    'meditite', 'medicham', 'electrike', 'manectric', 'plusle', 'minun', 'volbeat', 'illumise',
    'roselia', 'gulpin', 'swalot', 'carvanha', 'sharpedo', 'wailmer', 'wailord', 'numel', 'camerupt',
    'torkoal', 'spoink', 'grumpig', 'spinda', 'trapinch', 'vibrava', 'flygon', 'cacnea', 'cacturne',
    'swablu', 'altaria', 'zangoose', 'seviper', 'lunatone', 'solrock', 'barboach', 'whiscash',
    'corphish', 'crawdaunt', 'baltoy', 'claydol', 'lileep', 'cradily', 'anorith', 'armaldo',
    'feebas', 'milotic', 'castform', 'kecleon', 'shuppet', 'banette', 'duskull', 'dusclops',
    'tropius', 'chimecho', 'absol', 'wynaut', 'snorunt', 'glalie', 'spheal', 'sealeo', 'walrein',
    'clamperl', 'huntail', 'gorebyss', 'relicanth', 'luvdisc', 'bagon', 'shelgon', 'salamence',
    'beldum', 'metang', 'metagross', 'regirock', 'regice', 'registeel', 'latias', 'latios',
    'kyogre', 'groudon', 'rayquaza', 'jirachi', 'deoxys', 'turtwig', 'grotle', 'torterra',
    'chimchar', 'monferno', 'infernape', 'piplup', 'prinplup', 'empoleon', 'starly', 'staravia',
    'staraptor', 'bidoof', 'bibarel', 'kricketot', 'kricketune', 'shinx', 'luxio', 'luxray',
    'budew', 'roserade', 'cranidos', 'rampardos', 'shieldon', 'bastiodon', 'burmy', 'wormadam',
    'mothim', 'combee', 'vespiquen', 'pachirisu', 'buizel', 'floatzel', 'cherubi', 'cherrim',
    'shellos', 'gastrodon', 'ambipom', 'drifloon', 'drifblim', 'buneary', 'lopunny', 'mismagius',
    'honchkrow', 'glameow', 'purugly', 'chingling', 'stunky', 'skuntank', 'bronzor', 'bronzong',
    'bonsly', 'mime-jr', 'happiny', 'chatot', 'spiritomb', 'gible', 'gabite', 'garchomp', 'munchlax',
    'riolu', 'lucario', 'hippopotas', 'hippowdon', 'skorupi', 'drapion', 'croagunk', 'toxicroak',
    'carnivine', 'finneon', 'lumineon', 'mantyke', 'snover', 'abomasnow', 'weavile', 'magnezone',
    'lickilicky', 'rhyperior', 'tangrowth', 'electivire', 'magmortar', 'togekiss', 'yanmega',
    'leafeon', 'glaceon', 'gliscor', 'mamoswine', 'porygon-z', 'gallade', 'probopass', 'dusknoir',
    'froslass', 'rotom', 'uxie', 'mesprit', 'azelf', 'dialga', 'palkia', 'heatran', 'regigigas',
    'giratina', 'cresselia', 'phione', 'manaphy', 'darkrai', 'shaymin', 'arceus', 'victini',
    'snivy', 'servine', 'serperior', 'tepig', 'pignite', 'emboar', 'oshawott', 'dewott', 'samurott',
    'patrat', 'watchog', 'lillipup', 'herdier', 'stoutland', 'purrloin', 'liepard', 'pansage',
    'simisage', 'pansear', 'simisear', 'panpour', 'simipour', 'munna', 'musharna', 'pidove',
    'tranquill', 'unfezant', 'blitzle', 'zebstrika', 'roggenrola', 'boldore', 'gigalith', 'woobat',
    'swoobat', 'drilbur', 'excadrill', 'audino', 'timburr', 'gurdurr', 'conkeldurr', 'tympole',
    'palpitoad', 'seismitoad', 'throh', 'sawk', 'sewaddle', 'swadloon', 'leavanny', 'venipede',
    'whirlipede', 'scolipede', 'cottonee', 'whimsicott', 'petilil', 'lilligant', 'basculin',
    'sandile', 'krokorok', 'krookodile', 'darumaka', 'darmanitan', 'maractus', 'dwebble', 'crustle',
    'scraggy', 'scrafty', 'sigilyph', 'yamask', 'cofagrigus', 'tirtouga', 'carracosta', 'archen',
    'archeops', 'trubbish', 'garbodor', 'zorua', 'zoroark', 'minccino', 'cinccino', 'gothita',
    'gothorita', 'gothitelle', 'solosis', 'duosion', 'reuniclus', 'ducklett', 'swanna', 'vanillite',
    'vanillish', 'vanilluxe', 'deerling', 'sawsbuck', 'emolga', 'karrablast', 'escavalier',
    'foongus', 'amoonguss', 'frillish', 'jellicent', 'alomomola', 'joltik', 'galvantula', 'ferroseed',
    'ferrothorn', 'klink', 'klang', 'klinklang', 'tynamo', 'eelektrik', 'eelektross', 'elgyem',
    'beheeyem', 'litwick', 'lampent', 'chandelure', 'axew', 'fraxure', 'haxorus', 'cubchoo',
    'beartic', 'cryogonal', 'shelmet', 'accelgor', 'stunfisk', 'mienfoo', 'mienshao', 'druddigon',
    'golett', 'golurk', 'pawniard', 'bisharp', 'bouffalant', 'rufflet', 'braviary', 'vullaby',
    'mandibuzz', 'heatmor', 'durant', 'deino', 'zweilous', 'hydreigon', 'larvesta', 'volcarona',
    'cobalion', 'terrakion', 'virizion', 'tornadus', 'thundurus', 'reshiram', 'zekrom', 'landorus',
    'kyurem', 'keldeo', 'meloetta', 'genesect', 'chespin', 'quilladin', 'chesnaught', 'fennekin',
    'braixen', 'delphox', 'froakie', 'frogadier', 'greninja', 'bunnelby', 'diggersby', 'fletchling',
    'fletchinder', 'talonflame', 'scatterbug', 'spewpa', 'vivillon', 'litleo', 'pyroar', 'flabebe',
    'floette', 'florges', 'skiddo', 'gogoat', 'pancham', 'pangoro', 'furfrou', 'espurr', 'meowstic',
    'honedge', 'doublade', 'aegislash', 'spritzee', 'aromatisse', 'swirlix', 'slurpuff', 'inkay',
    'malamar', 'binacle', 'barbaracle', 'skrelp', 'dragalge', 'clauncher', 'clawitzer', 'helioptile',
    'heliolisk', 'tyrunt', 'tyrantrum', 'amaura', 'aurorus', 'sylveon', 'hawlucha', 'dedenne',
    'carbink', 'goomy', 'sliggoo', 'goodra', 'klefki', 'phantump', 'trevenant', 'pumpkaboo',
    'gourgeist', 'bergmite', 'avalugg', 'noibat', 'noivern', 'xerneas', 'yveltal', 'zygarde',
    'diancie', 'hoopa', 'volcanion', 'rowlet', 'dartrix', 'decidueye', 'litten', 'torracat',
    'incineroar', 'popplio', 'brionne', 'primarina', 'pikipek', 'trumbeak', 'toucannon', 'yungoos',
    'gumshoos', 'grubbin', 'charjabug', 'vikavolt', 'crabrawler', 'crabominable', 'oricorio',
    'cutiefly', 'ribombee', 'rockruff', 'lycanroc', 'wishiwashi', 'mareanie', 'toxapex', 'mudbray',
    'mudsdale', 'dewpider', 'araquanid', 'fomantis', 'lurantis', 'morelull', 'shiinotic', 'salandit',
    'salazzle', 'stufful', 'bewear', 'bounsweet', 'steenee', 'tsareena', 'comfey', 'oranguru',
    'passimian', 'wimpod', 'golisopod', 'sandygast', 'palossand', 'pyukumuku', 'type-null',
    'silvally', 'minior', 'komala', 'turtonator', 'togedemaru', 'mimikyu', 'bruxish', 'drampa',
    'dhelmise', 'jangmo-o', 'hakamo-o', 'kommo-o', 'tapu-koko', 'tapu-lele', 'tapu-bulu', 'tapu-fini',
    'cosmog', 'cosmoem', 'solgaleo', 'lunala', 'nihilego', 'buzzwole', 'pheromosa', 'xurkitree',
    'celesteela', 'kartana', 'guzzlord', 'necrozma', 'magearna', 'marshadow', 'poipole', 'naganadel',
    'stakataka', 'blacephalon', 'zeraora', 'meltan', 'melmetal', 'grookey', 'thwackey', 'rillaboom',
    'scorbunny', 'raboot', 'cinderace', 'sobble', 'drizzile', 'inteleon', 'skwovet', 'greedent',
    'rookidee', 'corvisquire', 'corviknight', 'blipbug', 'dottler', 'orbeetle', 'nickit', 'thievul',
    'gossifleur', 'eldegoss', 'wooloo', 'dubwool', 'chewtle', 'drednaw', 'yamper', 'boltund',
    'rolycoly', 'carkol', 'coalossal', 'applin', 'flapple', 'appletun', 'silicobra', 'sandaconda',
    'cramorant', 'arrokuda', 'barraskewda', 'toxel', 'toxtricity', 'sizzlipede', 'centiskorch',
    'clobbopus', 'grapploct', 'sinistea', 'polteageist', 'hatenna', 'hattrem', 'hatterene',
    'impidimp', 'morgrem', 'grimmsnarl', 'obstagoon', 'perrserker', 'cursola', 'sirfetchd',
    'mr-rime', 'runerigus', 'milcery', 'alcremie', 'falinks', 'pincurchin', 'snom', 'frosmoth',
    'stonjourner', 'eiscue', 'indeedee', 'morpeko', 'cufant', 'copperajah', 'dracozolt', 'arctozolt',
    'dracovish', 'arctovish', 'duraludon', 'dreepy', 'drakloak', 'dragapult', 'zacian', 'zamazenta',
    'eternatus', 'kubfu', 'urshifu', 'zarude', 'regieleki', 'regidrago', 'glastrier', 'spectrier',
    'calyrex', 'wyrdeer', 'kleavor', 'ursaluna', 'basculegion', 'sneasler', 'overqwil', 'enamorus',
    'sprigatito', 'floragato', 'meowscarada', 'fuecoco', 'crocalor', 'skeledirge', 'quaxly',
    'quaxwell', 'quaquaval', 'lechonk', 'oinkologne', 'tarountula', 'spidops', 'nymble', 'lokix',
    'pawmi', 'pawmo', 'pawmot', 'tandemaus', 'maushold', 'fidough', 'dachsbun', 'smoliv', 'dolliv',
    'arboliva', 'squawkabilly', 'nacli', 'naclstack', 'garganacl', 'charcadet', 'armarouge',
    'ceruledge', 'tadbulb', 'bellibolt', 'wattrel', 'kilowattrel', 'maschiff', 'mabosstiff',
    'shroodle', 'grafaiai', 'bramblin', 'brambleghast', 'toedscool', 'toedscruel', 'klawf',
    'capsakid', 'scovillain', 'rellor', 'rabsca', 'flittle', 'espathra', 'tinkatink', 'tinkatuff',
    'tinkaton', 'wiglett', 'wugtrio', 'bombirdier', 'finizen', 'palafin', 'varoom', 'revavroom',
    'cyclizar', 'orthworm', 'glimmet', 'glimmora', 'greavard', 'houndstone', 'flamigo', 'cetoddle',
    'cetitan', 'veluza', 'dondozo', 'tatsugiri', 'annihilape', 'clodsire', 'farigiraf', 'dudunsparce',
    'kingambit', 'great-tusk', 'scream-tail', 'brute-bonnet', 'flutter-mane', 'slither-wing',
    'sandy-shocks', 'iron-treads', 'iron-bundle', 'iron-hands', 'iron-jugulis', 'iron-moth',
    'iron-thorns', 'frigibax', 'arctibax', 'baxcalibur', 'gimmighoul', 'gholdengo', 'wo-chien',
    'chien-pao', 'ting-lu', 'chi-yu', 'roaring-moon', 'iron-valiant', 'koraidon', 'miraidon',
    'walking-wake', 'iron-leaves', 'dipplin', 'poltchageist', 'sinistcha', 'okidogi', 'munkidori',
    'fezandipiti', 'ogerpon', 'archaludon', 'hydrapple', 'gouging-fire', 'raging-bolt', 'iron-boulder',
    'iron-crown', 'terapagos', 'pecharunt',
]

# Extra spellings canon() can produce from real card names that the slugs miss.
#   "Farfetch'd" -> "farfetch d" (slug is "farfetchd")
#   bare "Nidoran" with no gender symbol
EXTRA_SPECIES_ALIASES = ["farfetch d", "sirfetch d", "nidoran"]

# Trainer/item cards whose names CONTAIN a species name — must lose to the
# trainer filter despite matching a species.
TRAINER_CARDS_WITH_SPECIES = [
    "clefairy doll", "rotom phone", "rotom bike", "rotom dex", "rotom catalog",
]

# English sets released BEFORE 2022 — blocked. NAMES COME FROM COLLECTR'S REAL
# EXPORT VOCABULARY (668 distinct set names audited from Kevin's CSVs, Aug 2):
# Collectr often prefixes "Pokemon ", era-prefixes names ("Sword & Shield
# Evolving Skies", "Xy Evolutions"), and names promo sets "<Era> Promo(s)" —
# never "Black Star Promos" TCGplayer-style. Matching handles all of those.
PRE_2022_SETS = [
    # WotC era (1999-2003) — Collectr also uses "Pokemon Game" for Base Set,
    # "Rocket" for Team Rocket, "Game Base II" for Base Set 2
    "Base Set", "Base Set Shadowless", "Base Set 2", "Jungle", "Fossil",
    "Team Rocket", "Rocket", "Gym Heroes", "Gym Challenge",
    "Game", "Game Base II", "Game Movie", "Game Promo",
    "Neo Genesis", "Neo Discovery", "Neo Revelation", "Neo Destiny",
    "Legendary Collection", "Expedition", "Expedition Base Set",
    "Aquapolis", "Skyridge", "Southern Islands", "Best of Game",
    "Wizards Black Star Promos", "Wizards of the Coast Promos", "WoTC Promo",
    "2000 Movie Promo", "2-Player CD-Rom Starter Set",
    "Topps Chrome Pokemon T.V.", "Topps Pokemon Tv",
    "Topps Pokemon Tv Animation Clear Cards",
    # EX era (2003-2007)
    "Ruby & Sapphire", "Sandstorm", "Dragon", "Team Magma vs Team Aqua",
    "Team Magma Vs Team Aqua", "Hidden Legends", "FireRed & LeafGreen",
    "Fire Red & Leaf Green", "Team Rocket Returns", "Deoxys",
    "Emerald", "Unseen Forces", "Delta Species", "Legend Maker",
    "Holon Phantoms", "Crystal Guardians", "Dragon Frontiers", "Power Keepers",
    "Battle Stadium", "Nintendo Black Star Promos",
    # DP / Platinum / HGSS (2007-2011)
    "Diamond & Pearl", "Mysterious Treasures", "Secret Wonders",
    "Great Encounters", "Majestic Dawn", "Legends Awakened", "Stormfront",
    "Platinum", "Rising Rivals", "Supreme Victors", "Arceus",
    "HeartGold & SoulSilver", "Unleashed", "Undaunted", "Triumphant",
    "Call of Legends",
    "POP Series 1", "POP Series 2", "POP Series 3", "POP Series 4",
    "POP Series 5", "POP Series 6", "POP Series 7", "POP Series 8",
    "POP Series 9", "Pokemon Rumble",
    # Black & White era (2011-2013)
    "Black & White", "Emerging Powers", "Noble Victories", "Next Destinies",
    "Dark Explorers", "Dragons Exalted", "Dragon Vault", "Boundaries Crossed",
    "Plasma Storm", "Plasma Freeze", "Plasma Blast", "Legendary Treasures",
    "Legendary Treasures Radiant Collection",
    # XY era (2014-2016)
    "Kalos Starter Set", "XY", "Flashfire", "Furious Fists", "Phantom Forces",
    "Primal Clash", "Double Crisis", "Roaring Skies", "Ancient Origins",
    "BREAKthrough", "BREAKpoint", "Generations", "Generations Radiant Collection",
    "Fates Collide", "Steam Siege", "Evolutions",
    # Sun & Moon era (2017-2019)
    "Sun & Moon", "Guardians Rising", "Burning Shadows", "Shining Legends",
    "Crimson Invasion", "Ultra Prism", "Forbidden Light", "Celestial Storm",
    "Dragon Majesty", "Lost Thunder", "Team Up", "Detective Pikachu",
    "Unbroken Bonds", "Unified Minds", "Hidden Fates",
    "Hidden Fates Shiny Vault", "Cosmic Eclipse",
    # Sword & Shield era, 2020-2021 only (2022+ SWSH sets stay ALLOWED:
    # Brilliant Stars, Astral Radiance, Pokemon GO, Lost Origin,
    # Silver Tempest, Crown Zenith)
    "Sword & Shield", "Rebel Clash", "Darkness Ablaze", "Champion's Path",
    "Vivid Voltage", "Shining Fates", "Shining Fates Shiny Vault",
    "Battle Styles", "Chilling Reign", "Evolving Skies", "Celebrations",
    "Celebrations Classic Collection", "Celebrations Ultra-Premium Collection",
    "Fusion Strike", "Battle Academy", "First Partner Pack", "Futsal",
    "Prize Pack Series One", "25th Anniversary",
    "McDonald's 25th Anniversary", "McDonald's 25th Anniversary Promos",
    "McDonald's Collection",
    "McDonald's Collection 2011", "McDonald's Collection 2012",
    "McDonald's Collection 2013", "McDonald's Collection 2014",
    "McDonald's Collection 2015", "McDonald's Collection 2016",
    "McDonald's Collection 2017", "McDonald's Collection 2018",
    "McDonald's Collection 2019", "McDonald's Collection 2021",
    # Generic promo buckets (all pre-2022 eras; the modern ones Collectr names
    # "Scarlet & Violet Promo" / "Sv Black Star Promo" / "Me Black Star Promo"
    # and those never match these entries)
    "Black Star Promo", "Black Star Promos", "Promo Black Star",
    "Black Star Promos - Sun & Moon",
    "League Promo", "League Promo Black Star", "League & Championship Cards",
    "World Championships Promo", "Mega Powers Collection Promo",
    "Premium Trainer Xy Collection Promo",
    "Asia 25th Anniversary Promo", "League Energize Your Game Cycle",
    "Deck Exclusives",
]

# Buckets that pool products from EVERY era ("Miscellaneous Cards & Products"
# holds a 2016 Mega Charizard UPC and a 2025 Destined Rivals stamped promo).
# A row here is dated by its PRODUCT NAME instead: a reference to a known
# 2022+ set allows it ("... (Destined Rivals Stamp)"); anything else stays
# blocked (fail closed — Kevin can override in the ticket).
UNDATABLE_SETS = ["Miscellaneous Cards & Products", "Jumbo Cards", "Promo"]

# Known 2022+ English sets, for dating undatable-bucket rows by name reference.
MODERN_SET_REFERENCES = [
    "Brilliant Stars", "Astral Radiance", "Pokemon GO", "Lost Origin",
    "Silver Tempest", "Crown Zenith", "Scarlet & Violet", "Paldea Evolved",
    "Obsidian Flames", "151", "Paradox Rift", "Paldean Fates",
    "Temporal Forces", "Twilight Masquerade", "Shrouded Fable",
    "Stellar Crown", "Surging Sparks", "Prismatic Evolutions",
    "Journey Together", "Destined Rivals", "Black Bolt", "White Flare",
    "Mega Evolution", "Phantasmal Flames", "Perfect Order", "Chaos Rising",
    "Ascended Heroes", "Pitch Black", "Trick or Trade",
]

# "Sword & Shield Promo" spans 2019-2022, so it's gated by PROMO NUMBER, not
# blanket-blocked: SWSH numbering is chronological and the 2022 run starts at
# SWSH185 (the Brilliant Stars Build & Battle stamps, Feb 2022 — verified
# against pokemon-tcg-data, whose F-regulation run starts exactly at SWSH185;
# SWSH179-184 are the Sept 2021 Eevee VMAX boxes). >=185 is 2022+; a row with
# no parseable SWSH number stays blocked (fail closed).
SWSH_PROMO_2022_FIRST = 185
_SWSH_PROMO_SET_NAMES = (
    "sword and shield promo", "sword and shield promos",
    "swsh promo", "swsh promos",
    "swsh black star promo", "swsh black star promos",
    "sword and shield black star promo", "sword and shield black star promos",
    "black star promos sword and shield", "black star promo sword and shield",
)

# JAPANESE-ONLY sets that Collectr lists with plain Latin names and NO
# "Japanese" marker (most JP rows say "... Japanese" or "Pokemon Japanese ..."
# and are caught by the language-token check instead). English-only rule.
JAPANESE_ONLY_SETS = [
    "Shiny Star V", "VMAX Climax", "VSTAR Universe", "Silver Lance",
    "Tag Team GX All Stars", "Eevee Heroes", "Fusion Arts", "Dream League",
    "Alter Genesis", "Shiny Treasure ex", "Terastal Festival ex",
    "Terastal Fest ex", "Triplet Beat", "Snow Hazard", "Clay Burst",
    "Raging Surf", "Ancient Roar", "Future Flash", "Crimson Haze",
    "Wild Force", "Cyber Judge", "Transformation Mask", "Mask of Change",
    "Night Wanderer", "Stellar Miracle", "Paradise Dragona",
    "Super Electric Breaker", "Heat Wave Arena", "Battle Partners",
    "Glory of Team Rocket", "Glory of the Rocket Gang", "Inferno X",
    "Ninja Spinner", "Nihil Zero", "Nullifying Zero", "Mega Brave",
    "Mega Symphonia", "MEGA Dream ex", "Scarlet ex", "Violet ex",
    "Ruler of the Black Flame", "Star Birth", "Gem Pack", "Gem Pack Vol. 3",
    "Brave Stars", "Hot Air Arena", "Towering Perfection", "Perfect Skyscraper",
    "Dark Shadow of the Blue Sea",
]

# Language tokens in a SET name that mark a non-English catalog row even when
# the card name itself is Latin script ("Pokemon Japanese Fossil",
# "Clay Burst - sv2D Japanese", "Scarlet & Violet Promo JP", "Mew Fr-151").
_LANGUAGE_TOKENS = frozenset([
    "japanese", "japan", "jp", "jpn", "korean", "kor", "chinese",
    "thai", "indonesian", "german", "french", "fr", "italian", "spanish",
    "portuguese",
])

# Sealed/oddball product words in a PRODUCT name — these are not raw singles
# even when the title contains a species name ("Pikachu V Box",
# "Mega Charizard X EX UPC Promo Sealed"). Token-boundary matched.
_SEALED_PRODUCT_TOKENS = frozenset([
    "sealed", "box", "boxes", "tin", "tins", "upc", "etb", "bundle",
    "blister", "booster", "boosters", "pack", "packs", "deck", "decks",
    "display", "case", "collection", "collections", "portfolio", "binder",
    "lot", "jumbo",
])

# Era names that may prefix a set name ("Sword & Shield Evolving Skies",
# "Xy Evolutions", "Platinum Arceus"). Stripping one and matching the
# remainder is safe; blind suffix-matching is NOT ("Prismatic Evolutions"
# ends with "Evolutions" but is a 2025 set).
_ERA_PREFIXES = (
    "sword and shield", "swsh", "sun and moon", "sm", "xy",
    "black and white", "bw", "heartgold and soulsilver",
    "heartgold soulsilver", "hgss", "diamond and pearl", "dp",
    "platinum", "ex", "e card", "neo",
)

# Suffix words that name a VARIANT of the same (blocked) set: "Neo Genesis
# 1st Edition", "Base Set (Unlimited)", "Sun & Moon Promo", "Diamond and
# Pearl Promos", "Southern Islands Promo".
_VARIANT_SUFFIX_TOKENS = frozenset([
    "1st", "edition", "unlimited", "shadowless", "promo", "promos", "and",
])


def canon(text):
    """Fold a card/set name to a canonical compare form: lowercase, accents
    stripped (Flabebe, Pokemon GO), gender symbols to f/m (Nidoran), "&" to
    "and", every non-alphanumeric run collapsed to a single space. The same
    function is applied to our data lists and to Collectr's values, so both
    sides always agree."""
    text = str(text or "").lower().replace("♀", " f ").replace("♂", " m ")
    # "&" and "and" must compare equal: "Sword & Shield" == "Sword and Shield"
    text = text.replace("&", " and ")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return " ".join("".join(c if (c.isalnum() and ord(c) < 128) else " " for c in text).split())


_SPECIES_CANON = frozenset(canon(s) for s in SPECIES_SLUGS) | frozenset(EXTRA_SPECIES_ALIASES)
_TRAINER_WITH_SPECIES_CANON = frozenset(canon(s) for s in TRAINER_CARDS_WITH_SPECIES)
_BLOCKED_SET_CANON = frozenset(canon(s) for s in PRE_2022_SETS)
_JAPANESE_SET_CANON = frozenset(canon(s) for s in JAPANESE_ONLY_SETS)
_UNDATABLE_SET_CANON = frozenset(canon(s) for s in UNDATABLE_SETS)
_MODERN_REFERENCE_CANON = frozenset(canon(s) for s in MODERN_SET_REFERENCES)


def find_species(product_name):
    """Return the (canon) species name contained in the product name, or None.
    Token-boundary containment: "Radiant Charizard" -> "charizard";
    "Iono" / "Ultra Ball" / "Basic Fire Energy" -> None. Longest match wins
    so "Mr. Rime" reports "mr rime", not a shorter accidental hit."""
    padded = " " + canon(product_name) + " "
    best = None
    for sp in _SPECIES_CANON:
        if " " + sp + " " in padded and (best is None or len(sp) > len(best)):
            best = sp
    return best


def is_trainer_with_species_name(product_name):
    """True for known trainer/item cards that contain a species name."""
    padded = " " + canon(product_name) + " "
    return any(" " + t + " " in padded for t in _TRAINER_WITH_SPECIES_CANON)


def is_sealed_product_name(product_name):
    """True when the product name reads as sealed/oddball product rather than
    a raw single ("Pikachu V Box", "... UPC Promo Sealed", "Booster Bundle").
    Needed because species-titled sealed items pass the trainer filter."""
    return any(t in _SEALED_PRODUCT_TOKENS for t in canon(product_name).split())


def is_master_ball_variant(product_name, variance):
    """Master Ball pattern versions are excluded (name or Variance column)."""
    return "master ball" in canon(product_name) or "master ball" in canon(variance)


def _strip_pokemon_prefix(c):
    """Collectr prefixes many set names with "Pokemon " ("Pokemon Fossil",
    "Pokemon Sword & Shield Evolving Skies"). One leading token only —
    "Pokemon Go" -> "go" is still unambiguous for OUR lists."""
    return c[8:] if c.startswith("pokemon ") else c


def is_non_english_set(set_name):
    """True when the SET name marks a non-English catalog: a language token
    anywhere in it, or a known Japanese/Chinese-only set name (with or without
    the "Pokemon " prefix; subtitle suffixes like "Brave Stars (Charm)" count)."""
    c = canon(set_name)
    if any(t in _LANGUAGE_TOKENS for t in c.split()):
        return True
    c = _strip_pokemon_prefix(c)
    return any(c == j or c.startswith(j + " ") for j in _JAPANESE_SET_CANON)


def _blocked(c):
    """Exact block-list check plus variant-suffix forms ("Neo Genesis 1st
    Edition", "Sun & Moon Promo", "Base Set (Unlimited)")."""
    if c in _BLOCKED_SET_CANON:
        return True
    for b in _BLOCKED_SET_CANON:
        if c.startswith(b + " ") and all(
                t in _VARIANT_SUFFIX_TOKENS for t in c[len(b) + 1:].split()):
            return True
    return False


def _swsh_promo_number(card_number):
    m = re.search(r'swsh\s*0*(\d+)', str(card_number or ''), re.IGNORECASE)
    return int(m.group(1)) if m else None


def _has_modern_reference(product_name):
    padded = " " + canon(product_name) + " "
    return any(" " + m + " " in padded for m in _MODERN_REFERENCE_CANON)


def is_pre_2022_set(set_name, card_number="", product_name=""):
    """True if the row is from a known pre-2022 English set (Collectr naming).
    Unknown -> False (assumed newer, allowed — new sets keep working with no
    code change). Scarlet & Violet / SV era is entirely 2023+. Two sets need
    row context: "Sword & Shield Promo" is gated by SWSH promo number (>=185
    is 2022+), and undatable buckets ("Miscellaneous Cards & Products") pass
    only when the product name references a known 2022+ set."""
    c = _strip_pokemon_prefix(canon(set_name))
    if not c:
        return False
    if c.startswith("scarlet and violet") or c.startswith("sv ") or c == "sv":
        return False
    if c in _SWSH_PROMO_SET_NAMES:
        n = _swsh_promo_number(card_number)
        return n is None or n < SWSH_PROMO_2022_FIRST
    if c in _UNDATABLE_SET_CANON:
        return not _has_modern_reference(product_name)
    if _blocked(c):
        return True
    for p in _ERA_PREFIXES:
        if c.startswith(p + " ") and _blocked(c[len(p) + 1:]):
            return True
    return False
