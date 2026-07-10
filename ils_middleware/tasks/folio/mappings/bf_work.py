"""SPARQL queries to extract data from a RDF Graph made up json-ld from Sinopia
BF Instance with its associated BF Work.
"""

alternative_title = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>

SELECT DISTINCT ?main_title ?subtitle ?part_number ?part_name
WHERE {{
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:title ?title .
    ?title a {bf_class} .
    ?title bf:mainTitle ?main_title .
    OPTIONAL {{ ?title bf:subtitle ?subtitle . }}
    OPTIONAL {{ ?title bf:partNumber ?part_number . }}
    OPTIONAL {{ ?title bf:partName ?part_name . }}
}}
"""

classification = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>

SELECT DISTINCT ?class_number
WHERE {{
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:classification ?class_node .
    ?class_node a {bf_class} .
    ?class_node bf:classificationPortion ?class_number .
}}
"""

contributor = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX bflc: <http://id.loc.gov/ontologies/bflc/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?agent ?role
WHERE {{
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:contribution ?contrib_bnode .
    ?contrib_bnode a bf:Contribution .
    FILTER NOT EXISTS {{ ?contrib_bnode a bf:PrimaryContribution }}
    ?contrib_bnode bf:role ?role_uri .
    ?role_uri rdfs:label ?role .
    ?contrib_bnode bf:agent ?agent_uri .
    ?agent_uri a {bf_class} .
    OPTIONAL {{ ?agent_uri rdfs:label ?agent_tagged . FILTER(lang(?agent_tagged) != "") }}
    OPTIONAL {{ ?agent_uri rdfs:label ?agent_plain . FILTER(lang(?agent_plain) = "") }}
    BIND(COALESCE(?agent_tagged, ?agent_plain) AS ?agent)
}}
"""

editions = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?edition
WHERE {{
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:editionStatement ?edition .
}}
"""

instance_type_id = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>

SELECT DISTINCT ?instance_type_id
WHERE {{
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:content ?instance_type .
    ?instance_type rdfs:label ?instance_type_id .
}}
"""

language = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>

SELECT DISTINCT ?language_uri ?language
WHERE {{
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:language ?language_uri .
    ?language_uri rdfs:label ?language .
}}
"""

primary_contributor = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX bflc: <http://id.loc.gov/ontologies/bflc/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?agent ?role
WHERE {{
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:contribution ?contrib_bnode .
    ?contrib_bnode a bf:PrimaryContribution .
    ?contrib_bnode bf:role ?role_uri .
    ?role_uri rdfs:label ?role .
    ?contrib_bnode bf:agent ?agent_uri .
    ?agent_uri a {bf_class} .
    OPTIONAL {{ ?agent_uri rdfs:label ?agent_tagged . FILTER(lang(?agent_tagged) != "") }}
    OPTIONAL {{ ?agent_uri rdfs:label ?agent_plain . FILTER(lang(?agent_plain) = "") }}
    BIND(COALESCE(?agent_tagged, ?agent_plain) AS ?agent)
}}
"""

series_controlled = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?series_title
WHERE {{
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:relation ?relation .
    ?relation a bf:Relation .
    ?relation bf:associatedResource ?hub .
    ?hub a bf:Hub .
    ?hub rdfs:label ?series_title .
}}
"""

summary = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?summary
WHERE {{
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:summary ?summary_bnode .
    ?summary_bnode a bf:Summary .
    ?summary_bnode rdfs:label ?summary .
}}
"""

series_uncontrolled = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?series_title
WHERE {{
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:relation ?relation .
    ?relation a bf:Relation .
    ?relation bf:associatedResource ?series .
    ?series a bf:Series .
    ?series bf:title ?title_node .
    ?title_node bf:mainTitle ?series_title .
}}
"""

subject = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?subject
WHERE {{
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:subject ?subject_node .
    OPTIONAL {{
        ?subject_node rdfs:label ?subject .
    }}
}}
"""

genre = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?genre
WHERE {{
    <{bf_work}> a bf:Work .
    <{bf_work}> bf:genreForm ?genre_node .
    OPTIONAL {{
        ?genre_node rdfs:label ?genre .
    }}
}}
"""
