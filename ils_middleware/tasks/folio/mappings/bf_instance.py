"""SPARQL queries to extract data from a RDF Graph made up json-ld from Sinopia
BF Instance with its associated BF Work.
"""

cataloged_date = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>

SELECT ?date
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:adminMetadata ?admin_metadata .
    ?admin_metadata a bf:AdminMetadata .
    ?admin_metadata bf:date ?date .
}}
"""

alternative_title = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?main_title ?subtitle ?part_number ?part_name
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:title ?title .
    ?title a {bf_class} .
    ?title bf:mainTitle ?main_title .
    OPTIONAL {{ ?title bf:subtitle ?subtitle . }}
    OPTIONAL {{ ?title bf:partNumber ?part_number . }}
    OPTIONAL {{ ?title bf:partName ?part_name . }}
}}
"""

editions = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?edition
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:editionStatement ?edition .
}}
"""

identifier = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?identifier
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:identifiedBy ?ident_bnode .
    ?ident_bnode a {bf_class} .
    ?ident_bnode rdf:value ?identifier .
}}
"""

instance_format_id = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?format_category ?format_term
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:media ?format_category_uri .
    <{bf_instance}> bf:carrier ?format_term_uri .
    ?format_category_uri rdfs:label ?format_category .
    ?format_term_uri rdfs:label ?format_term .
}}
"""

local_identifier = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?identifier
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:identifiedBy ?ident_bnode .
    ?ident_bnode a bf:Local .
    ?ident_bnode bf:source ?source_bnode .
    ?ident_bnode rdf:value ?identifier .
    ?source_bnode a bf:Source .
    OPTIONAL {{
        ?source_bnode rdfs:label "OColC" .
    }}
    OPTIONAL {{
        ?source_bnode rdfs:label "OCLC" .
    }}
}}
"""

electronic_locator = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>

SELECT ?url
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:electronicLocator ?url .
}}
"""

mode_of_issuance = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SElECT ?mode_of_issuance
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:issuance ?mode_of_issuance_uri .
    ?mode_of_issuance_uri rdfs:label ?mode_of_issuance
}}
"""

note = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?note
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:note ?note_bnode .
    ?note_bnode a bf:Note .
    ?note_bnode rdfs:label ?note .
}}
"""

physical_description = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?extent ?dimensions
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:extent ?extent_bnode .
    ?extent_bnode a bf:Extent .
    ?extent_bnode rdfs:label ?extent .
    OPTIONAL {{
        <{bf_instance}> bf:dimensions ?dimensions .
    }}
}}
"""

publication_frequency = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?frequency
WHERE {{
    <{bf_instance}> a bf:Instance .
    <{bf_instance}> bf:frequency ?freq_bnode .
    ?freq_bnode rdfs:label ?frequency .
}}
"""

publication = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX bflc: <http://id.loc.gov/ontologies/bflc/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?publisher ?date ?place
WHERE {{
   <{bf_instance}> a bf:Instance .
   <{bf_instance}> bf:provisionActivity ?activity .
   ?activity a bf:Publication .
   {{
       ?activity bflc:simpleAgent ?publisher .
       OPTIONAL {{ ?activity bflc:simpleDate ?date . }}
       OPTIONAL {{ ?activity bflc:simplePlace ?place . }}
   }} UNION {{
       ?activity bf:agent ?agent_uri .
       ?agent_uri rdfs:label ?publisher .
       OPTIONAL {{ ?activity bf:date ?date . }}
       OPTIONAL {{
           ?activity bf:place ?place_uri .
           ?place_uri rdfs:label ?place .
       }}
   }}
}}
"""

title = """PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?main_title ?subtitle ?part_number ?part_name
WHERE {{
  <{bf_instance}> a bf:Instance .
  <{bf_instance}> bf:title ?title .
  ?title a {bf_class} .
  ?title bf:mainTitle ?main_title .
  OPTIONAL {{
     ?title bf:subtitle ?subtitle .
  }}
  OPTIONAL {{
     ?title bf:partNumber ?part_number .
  }}
  OPTIONAL {{
     ?title bf:partName ?part_name
  }}
}}
"""
