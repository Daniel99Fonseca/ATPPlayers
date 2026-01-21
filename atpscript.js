/* ------------------------------------------------------------------------------------------------
  Para importar worldcities:
  mongoimport --db=atp --collection=worldcities --type=csv --headerline --file="worldcities.csv"
   ------------------------------------------------------------------------------------------------
*/

use("atp")

// Nomes duplicados encontrados e respetivos linkplayers para distinguir

const homonimos = [
  {
    "_id": "Mark Kovacs",
    "Links": [
      "https://www.atptour.com/en/players/mark-kovacs/k678/player-activity?year=all&matchType=Singles",
      "https://www.atptour.com/en/players/mark-kovacs/kb22/player-activity?year=all&matchType=Singles"
    ]
  },
  {
    "_id": "Martin Damm",
    "Links": [
      "https://www.atptour.com/en/players/martin-damm/d214/player-activity?year=all&matchType=Singles",
      "https://www.atptour.com/en/players/martin-damm/d0dt/player-activity?year=all&matchType=Singles"
    ]
  },
  {
    "_id": "Andreas Weber",
    "Links": [
      "https://www.atptour.com/en/players/andreas-weber/w449/player-activity?year=all&matchType=Singles",
      "https://www.atptour.com/en/players/andreas-weber/w237/player-activity?year=all&matchType=Singles"
    ]
  },
  {
    "_id": "Enrique Pena",
    "Links": [
      "https://www.atptour.com/en/players/enrique-pena/p0iz/player-activity?year=all&matchType=Singles",
      "https://www.atptour.com/en/players/enrique-pena/p306/player-activity?year=all&matchType=Singles"
    ]
  },
  {
    "_id": "Robert Phillips",
    "Links": [
      "https://www.atptour.com/en/players/robert-phillips/pd13/player-activity?year=all&matchType=Singles",
      "https://www.atptour.com/en/players/robert-phillips/p239/player-activity?year=all&matchType=Singles"
    ]
  },
  {
    "_id": "Alexey Nesterov",
    "Links": [
      "https://www.atptour.com/en/players/alexey-nesterov/n0ax/player-activity?year=all&matchType=Singles",
      "https://www.atptour.com/en/players/alexey-nesterov/n645/player-activity?year=all&matchType=Singles"
    ]
  },
  {
    "_id": "Alberto Gonzalez",
    "Links": [
      "https://www.atptour.com/en/players/alberto-gonzalez/g419/player-activity?year=all&matchType=Singles",
      "https://www.atptour.com/en/players/alberto-gonzalez/g975/player-activity?year=all&matchType=Singles"
    ]
  }
];

// Loop para aplicar as alterações a homonimos
homonimos.forEach(h => {
    db.atpplayers.updateMany(
        { "LinkPlayer": h.Links[0] },
        { $set: { "PlayerName": h._id + "-1" } } // O primeiro nome encontrado fica com -1 no nome
    );
    print("Atualizado: " + h._id + "-1");

    db.atpplayers.updateMany(
        { "LinkPlayer": h.Links[1] },
        { $set: { "PlayerName": h._id + "-2" } } // o segundo nome encontrado fica com -2 no nome
    );
    print("Atualizado: " + h._id + "-2");
});

/* ----------------------------------------------------------
  1. Construir o dicionário geo_dictionary usando worldcities
  -----------------------------------------------------------
*/ 

db.geo_dictionary.drop()  // DEvido a erros do MongoDB, é melhor apagar a coleção antes de recriá-la

db.worldcities.aggregate([
  {
    "$project": {
      "country": 1,
      "search_keys": ["$city", "$city_ascii", "$iso2", "$iso3", "$country", "$admin_name"]
    }
  },
  { "$unwind": "$search_keys" },
  {
    "$project": {
      "_id": { "$toLower": "$search_keys" }, // tornar tudo em minúsculas para abranger mais casos
      "country": 1
    }
  },
  { "$match": { "_id": { "$ne": null, "$ne": "" } } },
  // merge na coleção geo_dictionary
  { "$merge": { "into": "geo_dictionary", "on": "_id", "whenMatched": "replace", "whenNotMatched": "insert" } }
])

/* ----------------------------------------------------------
  2. Limpeza principal e normalização dos dados
  -----------------------------------------------------------
*/ 

db.mongoclean.drop() // devido a erros do MongoDB, é melhor apagar a coleção antes de recriá-la

// Proteção especial para USA, devido a estados coincidirem com ISO2 
const usa_protection = ["usa", "u.s.a.", "united states", "va", "fl", "ny", "tx", "ca", "pa", "oh", "il", "nc", "ga", "mi", "nj", "md", "dc", "d.c.", "washington", "ok", "texas", "hi", "ks", "ia", "or", "wi", "wv", "ct"];

db.atpplayers.aggregate([
  // apagar linhas onde o mesmo jogador aparece 2x para o mesmo jogo
  {
    "$group": {
      "_id": {  // criacao de um id composto
        "PlayerName": "$PlayerName",
        "Tournament": "$Tournament",
        "Date": "$Date",
        "Oponent": "$Oponent",
        "Score": "$Score"
      },
      "doc": { "$first": "$$ROOT" } // mantem apenas o primeiro registo encontrado
    }
  },
  { "$replaceRoot": { "newRoot": "$doc" } }, // substitui o documento original pelo documento limpo


  // Projeção inicial
  {
    "$project": {
      "_id": 0,
      "PlayerName": 1, 
      "Oponent": 1,
      "Tournament": 1, 
      "GameRound": 1, 
      "Ground": 1, 
      "WL": 1, 
      "Score": 1,
      "StartDate": { "$dateFromString": { "dateString": { "$trim": { "input": { "$arrayElemAt": [{ "$split": ["$Date", "-"] }, 0] } } }, "format": "%Y.%m.%d", "onError": null } },
      "EndDate": { "$dateFromString": { "dateString": { "$trim": { "input": { "$arrayElemAt": [{ "$split": ["$Date", "-"] }, 1] } } }, "format": "%Y.%m.%d", "onError": null } },
      "TornLoc_Search": { "$toLower": { "$trim": { "input": { "$arrayElemAt": [{ "$split": ["$Location", ","] }, -1] } } } },
      "Born_Search": { "$toLower": { "$trim": { "input": { "$arrayElemAt": [{ "$split": ["$Born", ","] }, -1] } } } },
      
      // correção da mão dominante
      "DomHand": { 
        "$switch": {
          "branches": [
            // Ambidestro
            { 
              "case": { "$regexMatch": { "input": { "$ifNull": ["$Hand", ""] }, "regex": "Ambidextrous", "options": "i" } }, 
              "then": "Ambidextrous"
            },
            // Esquerdino
            { 
              "case": { "$regexMatch": { "input": { "$ifNull": ["$Hand", ""] }, "regex": "Left", "options": "i" } }, 
              "then": "Left" 
            },
            // Destro
            { 
              "case": { "$regexMatch": { "input": { "$ifNull": ["$Hand", ""] }, "regex": "Right", "options": "i" } }, 
              "then": "Right" 
            }
          ],
          // null
          "default": "U" 
        } 
      }
    }
  },

  // Lookup no dicionário geo_dictionary
  { "$lookup": { "from": "geo_dictionary", "localField": "Born_Search", "foreignField": "_id", "as": "born_match" } },
  { "$lookup": { "from": "geo_dictionary", "localField": "TornLoc_Search", "foreignField": "_id", "as": "torn_match" } },

  // 
  {
    "$addFields": {
      "BornCountry_Temp": {
        "$switch": {
          "branches": [
            { "case": { "$in": ["$Born_Search", usa_protection] }, "then": "United States" },
            { "case": { "$and": [ { "$gt": [{ "$size": "$born_match" }, 0] }, { "$ne": [{ "$arrayElemAt": ["$born_match.country", 0] }, "Vatican City"] } ] }, "then": { "$arrayElemAt": ["$born_match.country", 0] } }
          ],
          "default": "$Born_Search"
        }
      },
      "TornLoc_Temp": {
        "$switch": {
          "branches": [
            { "case": { "$in": ["$TornLoc_Search", usa_protection] }, "then": "United States" },
            { "case": { "$gt": [{ "$size": "$torn_match" }, 0] }, "then": { "$arrayElemAt": ["$torn_match.country", 0] } }
          ],
          "default": "$TornLoc_Search"
        }
      }
    }
  },

  // Limpeza e normalização final
  {
    "$addFields": {
      
      // Normalização dos torneios
      "Tournament_Clean": {
        "$switch": {
          "branches": [
            { "case": { "$regexMatch": { "input": "$Tournament", "regex": "Australian Open", "options": "i" } }, "then": "Australian Open" },
            { "case": { "$regexMatch": { "input": "$Tournament", "regex": "US Open", "options": "i" } }, "then": "US Open" },
            { "case": { "$regexMatch": { "input": "$Tournament", "regex": "Wimbledon", "options": "i" } }, "then": "Wimbledon" },
            { "case": { "$regexMatch": { "input": "$Tournament", "regex": "Roland Garros|French Open", "options": "i" } }, "then": "Roland Garros" },

            // Normalização das variações de "vs"
            { 
               "case": { "$regexMatch": { "input": "$Tournament", "regex": " (v|vs|V|VS|Vs|v\\.|V\\.|vs\\.|VS\\.|Vs\\.|vS\\.) ", "options": "" } },
               "then": { 
                  "$replaceAll": { "input": { 
                    "$replaceAll": { "input": { 
                      "$replaceAll": { "input": { 
                        "$replaceAll": { "input": { 
                          "$replaceAll": { "input": { 
                            "$replaceAll": { "input": { 
                              "$replaceAll": { "input": { 
                                "$replaceAll": { "input": { 
                                  "$replaceAll": { "input": { 
                                    "$replaceAll": { "input": "$Tournament", "find": " vs. ", "replacement": " vs " } 
                                  }, "find": " v. ", "replacement": " vs " } 
                                }, "find": " V. ", "replacement": " vs " } 
                              }, "find": " Vs. ", "replacement": " vs " } 
                            }, "find": " VS. ", "replacement": " vs " } 
                          }, "find": " v ", "replacement": " vs " } 
                        }, "find": " V ", "replacement": " vs " } 
                      }, "find": " Vs ", "replacement": " vs " } 
                    }, "find": " VS ", "replacement": " vs " } 
                  }, "find": " vS ", "replacement": " vs " } 
               }
            },
            
            // CASO ESPECIAL: ASTANA-2 (Hífens em geral)
            { "case": { "$regexMatch": { "input": "$Tournament", "regex": "-", "options": "" } }, 
              "then": { "$replaceAll": { "input": "$Tournament", "find": "-", "replacement": " " } } 
            }
          ],
          "default": "$Tournament"
        }
      },

      // 2. BORN COUNTRY
      "BornCountry": {
        "$switch": {
          "branches": [
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "aktau", "options": "i" } }, "then": "Kazakhstan" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "cagayan de oro city", "options": "i" } }, "then": "Philippines" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "samosir island", "options": "i" } }, "then": "Indonesia" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "guimaraes|oliveira de azem", "options": "i" } }, "then": "Portugal" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "bras|s.o paulo|s.o bernardo|londrina|sao paulo", "options": "i" } }, "then": "Brazil" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "ciudad aut.noma|c.rdoba|villa maria|chascom", "options": "i" } }, "then": "Argentina" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "gr.felfing|l.beck|m.nchengladbach|t.bingen|.stringen", "options": "i" } }, "then": "Germany" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "hodon.n|hradec kr.lov.|pilsen", "options": "i" } }, "then": "Czechia" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "c.ret|s.vres|noum.a|li.ge|le pont-de", "options": "i" } }, "then": "France" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "li.ge", "options": "i" } }, "then": "Belgium" }, 
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "hyvink", "options": "i" } }, "then": "Finland" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "eksj", "options": "i" } }, "then": "Sweden" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "ciri", "options": "i" } }, "then": "Italy" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "paysand", "options": "i" } }, "then": "Uruguay" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "taipei city", "options": "i" } }, "then": "Taiwan" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "korea|an sung|gangwonto|kyunggi", "options": "i" } }, "then": "South Korea" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "usa|u\\.s\\.a|united s|atlanta|chandler|durham|greenbrae|humboldt|lithonia|new york|tarzana|winston|maryland|michigan|ohio|texas|tennessee|north carolina|illinois|idaho|ok|princeton|haiti|pa u\\.s\\.a|ca usa|does moines|saipan", "options": "i" } }, "then": "United States" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "ger|bergisch|duesseldorf|muenster|nurtingen|rotenburg|schwaebisch|weiden", "options": "i" } }, "then": "Germany" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "uk|england|britain|scotland|wales|devon|london|newcastle|st albans", "options": "i" } }, "then": "United Kingdom" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "c\\.ret|s\\.vres|noum\\.a|carhaix|pont-de|levallois|lons|nogent|pouillon|saint|schoelcher|sucy|tahiti|villeneuve|reunion|uriage", "options": "i" } }, "then": "France" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "genova|aquila|milano|rovato|sanremo|torino|reggio|sardinia|ciri", "options": "i" } }, "then": "Italy" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "algemesi|ciutadella|gijon|palma|sant carles|spai|bakio|melilla|valldoreix", "options": "i" } }, "then": "Spain" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "lodz|ostrow|priemysl|tomaszow|walbrzych|warszawa|wrocklaw|wroclaw|zielona", "options": "i" } }, "then": "Poland" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "bacau|bucuresti|constanta|iasi|pitesti|ploiesti|rm valcea|rumania", "options": "i" } }, "then": "Romania" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "soviet|korolev|north-ossetia|orenbourg|rostov|saint-peter", "options": "i" } }, "then": "Russia" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "czech|slovak|presov|hodon|hradec|kolin|moravska|pilsen|valasske|presov|cz republic", "options": "i" } }, "then": "Czechia" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "crimea|dnepropetrovsk|evpatoria|kiev|krivoy|novaya", "options": "i" } }, "then": "Ukraine" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "bosnia", "options": "i" } }, "then": "Bosnia and Herzegovina" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "yug.|yugoslavia|scg|sergia", "options": "i" } }, "then": "Serbia" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "solvenia|portoroz", "options": "i" } }, "then": "Slovenia" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "crotia", "options": "i" } }, "then": "Croatia" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "chinese ta|tpe|changhwa|chunghua|new taipei", "options": "i" } }, "then": "Taiwan" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "bengaluru|haryana|hyderabad|kolkata|visakhapatnam", "options": "i" } }, "then": "India" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "hyogo|hyougo|kanagawa|nishinomiya|osaka|saitma", "options": "i" } }, "then": "Japan" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "heilongjiang|hunan|jiangsu|sichuan|xin jiang", "options": "i" } }, "then": "China" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "argent|arg\\.|capital federal|buenos aires|mar del|marco juarez", "options": "i" } }, "then": "Argentina" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "aust\\.|aust\\.\\.|st kilda|box hill|monto|subiaco|tasmania|s\\.a\\. australia", "options": "i" } }, "then": "Australia" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "ned|nethe|dirksland|heythuysen|elndhoven|'s-hertogenbosch|the netherlands", "options": "i" } }, "then": "Netherlands" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "leuven|woluwe|liege|li.ge", "options": "i" } }, "then": "Belgium" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "hjo|lund|eksj", "options": "i" } }, "then": "Sweden" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "switz|bienne|neuchatel", "options": "i" } }, "then": "Switzerland" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "aut\\.|portschach|wr\\.neustadt", "options": "i" } }, "then": "Austria" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "den\\.|frederiksberg", "options": "i" } }, "then": "Denmark" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "hyvink", "options": "i" } }, "then": "Finland" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "hunary", "options": "i" } }, "then": "Hungary" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "barakovichi", "options": "i" } }, "then": "Belarus" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "canda|alberta|ontaria|montreal", "options": "i" } }, "then": "Canada" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "meixco|mexica|acapulco|queretaro", "options": "i" } }, "then": "Mexico" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "south africa|rsa|gauteng|johanesburg|port elizabeth", "options": "i" } }, "then": "South Africa" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "providencia|bolivia", "options": "i" } }, "then": "Chile" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "venezeuela", "options": "i" } }, "then": "Venezuela" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "uru|uraguay|paysand", "options": "i" } }, "then": "Uruguay" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "isra|hasharon|petach", "options": "i" } }, "then": "Israel" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "izmir|tekirdag", "options": "i" } }, "then": "Turkey" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "egpyt|eygpt|sharm", "options": "i" } }, "then": "Egypt" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "dominican|d\\.r\\.|santiago de los", "options": "i" } }, "then": "Dominican Republic" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "new caledonia|new caledoni|noum", "options": "i" } }, "then": "New Caledonia" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "new zealan", "options": "i" } }, "then": "New Zealand" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "macedonia|macedona", "options": "i" } }, "then": "North Macedonia" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "curacao|dutch anti", "options": "i" } }, "then": "Curaçao" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "habana", "options": "i" } }, "then": "Cuba" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "cote|ivory coast", "options": "i" } }, "then": "Côte d’Ivoire" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "esch/alzette|mondorf", "options": "i" } }, "then": "Luxembourg" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "moldova|moldovia", "options": "i" } }, "then": "Moldova" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "myanmar", "options": "i" } }, "then": "Myanmar" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "monte carlo", "options": "i" } }, "then": "Monaco" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "uae|u\\.a\\.e\\.", "options": "i" } }, "then": "United Arab Emirates" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "trinidad|tri", "options": "i" } }, "then": "Trinidad and Tobago" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "botwana", "options": "i" } }, "then": "Botswana" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "bahamas|grand bahamas", "options": "i" } }, "then": "Bahamas" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "haiti|port au prince", "options": "i" } }, "then": "Haiti" },
            { "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "camboda", "options": "i" } }, "then": "Cambodia" },
            
            { 
              "case": { 
                "$in": ["$BornCountry_Temp", ["", null, "caribbean", "lesser antilles", "pacific oceania", "tba", "tbc", "tbd", "south", "unknown"]] 
              }, 
              "then": "País não definido" 
            },

            // Se a string for vazia ou apenas espaços, define como "País não definido"
            { 
              "case": { "$regexMatch": { "input": "$BornCountry_Temp", "regex": "^\\s*$", "options": "" } },
              "then": "País não definido" 
            }
          ],
           
          // Se por acaso o Temp (Temporário) for nulo (falha no dicionário), forçamos "País não definido"
          "default": { "$ifNull": ["$BornCountry_Temp", "País não definido"] }
        }
      },
      
      // TORNLOC
      "TornLoc": {
        "$switch": {
          "branches": [
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "arg\\.|argent", "options": "i" } }, "then": "Argentina" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "bosnia", "options": "i" } }, "then": "Bosnia and Herzegovina" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "britain|england|scotland|wales|uk", "options": "i" } }, "then": "United Kingdom" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "curacao|dutch anti", "options": "i" } }, "then": "Curaçao" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "czech|repub", "options": "i" } }, "then": "Czechia" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "korea", "options": "i" } }, "then": "South Korea" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "meixco|mexica", "options": "i" } }, "then": "Mexico" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "ned|the nethe|elndhoven", "options": "i" } }, "then": "Netherlands" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "phi|phillipines|manilla", "options": "i" } }, "then": "Philippines" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "serbia|scg|sergia", "options": "i" } }, "then": "Serbia" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "slovak|slovkia", "options": "i" } }, "then": "Slovakia" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "u\\.s\\.a|united s", "options": "i" } }, "then": "United States" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "usa|new york", "options": "i" } }, "then": "United States" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "brasil|braz|sao paulo|rio de janeiro|florianapolis", "options": "i" } }, "then": "Brazil" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "nethe|ned|'s-hertogenbosch", "options": "i" } }, "then": "Netherlands" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "uru|uraguay", "options": "i" } }, "then": "Uruguay" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "venezeuela", "options": "i" } }, "then": "Venezuela" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "egpyt|eygpt|sharm", "options": "i" } }, "then": "Egypt" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "mas|kuala", "options": "i" } }, "then": "Malaysia" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "switz", "options": "i" } }, "then": "Switzerland" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "isra|hasharon", "options": "i" } }, "then": "Israel" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "soviet", "options": "i" } }, "then": "Russia" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "u\\.a\\.e\\.", "options": "i" } }, "then": "United Arab Emirates" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "trinidad|tri", "options": "i" } }, "then": "Trinidad and Tobago" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "uzb\\.", "options": "i" } }, "then": "Uzbekistan" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "den\\.", "options": "i" } }, "then": "Denmark" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "aut\\.|portschach", "options": "i" } }, "then": "Austria" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "solvenia|portoroz", "options": "i" } }, "then": "Slovenia" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "wrocklaw", "options": "i" } }, "then": "Poland" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "crotia", "options": "i" } }, "then": "Croatia" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "hunary", "options": "i" } }, "then": "Hungary" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "botwana", "options": "i" } }, "then": "Botswana" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "new caledoni", "options": "i" } }, "then": "New Caledonia" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "new zealan", "options": "i" } }, "then": "New Zealand" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "macedona", "options": "i" } }, "then": "North Macedonia" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "ivory coast", "options": "i" } }, "then": "Côte d’Ivoire" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "chinese ta|tpe|taiwan", "options": "i" } }, "then": "Taiwan" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "yug\\.|yugoslavia", "options": "i" } }, "then": "Serbia" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "angleur|liege", "options": "i" } }, "then": "Belgium" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "bakio|valldoreix|palma de", "options": "i" } }, "then": "Spain" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "bolivia|santa cruz", "options": "i" } }, "then": "Bolivia" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "canda|ontaria", "options": "i" } }, "then": "Canada" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "habana", "options": "i" } }, "then": "Cuba" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "d\\.r\\.|domincan", "options": "i" } }, "then": "Dominican Republic" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "esch|mondorf", "options": "i" } }, "then": "Luxembourg" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "fiji", "options": "i" } }, "then": "Fiji" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "moldovia", "options": "i" } }, "then": "Moldova" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "myanmar", "options": "i" } }, "then": "Myanmar" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "novaya", "options": "i" } }, "then": "Ukraine" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "reggio|sardinia", "options": "i" } }, "then": "Italy" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "reunion|uriage", "options": "i" } }, "then": "France" },
            { "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "south\\s+africa", "options": "i" } }, "then": "South Africa" },
            
            { 
              "case": { 
                "$in": ["$TornLoc_Temp", ["", null, "caribbean", "lesser antilles", "pacific oceania", "tba", "tbc", "tbd", "south", "unknown"]] 
              }, 
              "then": "País não definido" 
            },

            // Se for string vazia ou só espaços -> "País não definido"
            { 
              "case": { "$regexMatch": { "input": "$TornLoc_Temp", "regex": "^\\s*$", "options": "" } },
              "then": "País não definido" 
            }
          ],
          
          // Default: Se o Temp for nulo ou inválido, passa para "País não definido"
          "default": { "$ifNull": ["$TornLoc_Temp", "País não definido"] }
        }
      }
    }
  },

  // --- OUTPUT FINAL ---
  {
    "$project": {
      "PlayerName": 1, 
      "BornCountry": 1, 
      "TornLoc": 1, 
      "Tournament": "$Tournament_Clean",
      "DomHand": 1, 
      "Ground": 1, 
      "WL": 1,
      "StartDate": 1,
      "Oponent": 1,
      
      // CHAVE COMPOSTA (MATCH ID)
      "MatchID": { 
        "$concat": [
          "$Tournament_Clean", "|", 
          "$GameRound", "|", 
          { "$toString": "$StartDate" }, "|",
          // Lógica: Se W -> PlayerName, Se L -> Oponent
          {
            "$cond": {
            "if": {
              // Condição: O último componente é o PlayerName se WL for "W" ou se WL estiver vazio/nulo.
              "$or": [
                { "$eq": ["$WL", "W"] }, // Se WL é uma vitória
                { "$in": ["$WL", ["", null]] } // OU se WL está vazio ou é null
              ]
            },
            // Então: Usa o nome do jogador atual
            "then": "$PlayerName",
            // Senão (Implica que se WL é "L"): Usa o nome do Oponente (o vencedor)
            "else": {
              // para segurança caso o campo Oponent não exista
              "$ifNull": ["$Oponent", "UNKNOWN_OPPONENT"]
            }
            }   
          }
        ] 
      }
    }
  },
  { "$out": "mongoclean" }
])


// --- CRIAÇÃO DE COLLECTIONS AUXILIARES (DEDUPLICADAS) ---

// 1. PLAYER INFO

db.player_info.drop()
db.mongoclean.aggregate([
  { 
    "$group": {
      "_id": "$PlayerName", // A chave única é o nome
      "DomHand": { "$first": "$DomHand" },  
      "BornCountry": { "$first": "$BornCountry" } 
    }
  },
  { 
    "$project": {
      "_id": 0,
      "PlayerName": "$_id", // Renomeia o _id de volta para PlayerName
      "DomHand": 1,
      "BornCountry": 1
    }
  },
  { "$out": "player_info" }
])

// 2. COUNTRIES 
db.countries.drop()
db.mongoclean.aggregate([
  { 
    "$group": { "_id": "$BornCountry" } // Agrupa por país
  },
  {
    "$project": { "_id": 0, "country": "$_id" }
  },
  { "$unionWith": { "coll": "mongoclean", "pipeline": [{ "$group": { "_id": "$TornLoc" } }, { "$project": { "country": "$_id" } }] } },
  { "$group": { "_id": "$country" } }, // Agrupa de novo após a união
  { "$project": { "_id": 0, "country": "$_id" } },
  { "$match": { "country": { "$ne": null } } }, // Remove nulos
  { "$out": "countries" }
])

// 3. TOURNAMENTS
// Um torneio é único pela combinação de Nome + Data
db.tournaments.drop()
db.mongoclean.aggregate([
  {
    "$group": {
      "_id": {
        "Tournament": "$Tournament",
        "StartDate": "$StartDate"
      },
      "TornLoc": { "$first": "$TornLoc" }
    }
  },
  {
    "$project": {
      "_id": 0,
      "Tournament": "$_id.Tournament",
      "StartDate": "$_id.StartDate",
      "TornLoc": 1
    }
  },
  { "$out": "tournaments" }
])

// 4. GAME INFO (Mantém-se igual, pois aqui queremos todos os jogos, mantendo apenas a informação necessária)
db.game_info.drop()
db.mongoclean.aggregate([
  {
    "$project": {
      "_id": 0,
      "MatchID": 1,
      "WL": 1,
      "Ground": 1,
      "PlayerName": 1,
      "Oponent": 1,
      "Tournament": 1,
    }
  },
  { "$out": "game_info" }
])