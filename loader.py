import os
import pandas as pd
import io
import re
from neo4j import GraphDatabase
from dotenv import load_dotenv

# .env 파일 로드 (비밀번호 등 접속 정보)
load_dotenv()

# 환경 변수 설정
URI = os.getenv("NEO4J_URI")
AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

# --- 1. 문서 내 데이터 (검증 통과된 데이터) ---
org_input_csv = """Org,Organization_Type,Headquarters_Location,Homepage,Sector_Industry,Roles,BusinessStatus,FoundedYear
14trees,PrivateCompany,CHE,14trees.com,3DPrinting;AEC,3DCPServiceProvider3DCP,Active,
holcim,PublicCompany,CHE,holcim.com,3DPrinting;AEC,3DCPMaterialsManufacturerProvider,Active,
apiscor,PrivateCompany,USA,apis-cor.com,3DPrinting;AEC,3DCPMaterialsDeveloper,Active,"""

location_code_csv = """CountryName,CountryCodeISO3166,CountryCodeUN,Predicate,Neo4jRelType
Switzerland,CHE,756,HasCountryCode,HAS_COUNTRY_CODE
United States of America,USA,840,HasCountryCode,HAS_COUNTRY_CODE"""

org_relations_csv = """SourceOrg,TargetOrg,Subtype,Business_Relation_Sources,Paper_Source,SourcePatentSources,TargetPatentSources
holcim,14trees,PartnersWith,https://www.holcim.com/who-we-are/our-stories/14trees,,,
holcim,14trees,IsShareholderOf,https://www.holcim.com/who-we-are/our-stories/14trees,,,
apiscor,holcim,CompetesWith,https://example.com/news,,,
"""

subtype_map_csv = """Subtype,neo4j_edge_type,category,direction
PartnersWith,COOPERATES_WITH,COOPERATION,A↔B
IsShareholderOf,INVESTED_IN,INVESTMENT,A→B
CompetesWith,COMPETES_WITH,COMPETITION,A↔B"""

# --- 2. Neo4j 적재 클래스 ---
class Neo4jLoader:
    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)
        self.orgs = pd.read_csv(io.StringIO(org_input_csv))
        self.locations = pd.read_csv(io.StringIO(location_code_csv))
        self.relations = pd.read_csv(io.StringIO(org_relations_csv))
        self.subtypes = pd.read_csv(io.StringIO(subtype_map_csv))

    def close(self):
        self.driver.close()

    def to_snake_case(self, text):
        """PascalCase -> SNAKE_CASE 변환 (예: IsLocatedIn -> IS_LOCATED_IN)"""
        if pd.isna(text): return None
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', str(text))
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).upper()

    def load_graph(self):
        with self.driver.session() as session:
            print("🚀 Neo4j에 데이터 적재를 시작합니다...")

            # 1) 국가 매핑 딕셔너리 생성 (CHE -> Switzerland)
            country_map = dict(zip(self.locations['CountryCodeISO3166'], self.locations['CountryName']))

            # 2) 조직(Organization) 및 국가(Country) 노드 생성
            for _, row in self.orgs.iterrows():
                country_name = country_map.get(row['Headquarters_Location'], "Unknown")
                
                # Cypher 쿼리: 조직 노드 생성 -> 국가 노드 생성 -> IS_LOCATED_IN 연결
                query = """
                MERGE (o:Organization {orgId: $orgId})
                SET o.name = $orgId, 
                    o.homepage = $homepage, 
                    o.status = $status,
                    o.updatedAt = datetime()
                MERGE (c:Country {name: $country})
                MERGE (o)-[:IS_LOCATED_IN]->(c)
                """
                session.run(query, orgId=row['Org'], 
                            homepage=row['Homepage'], 
                            status=row['BusinessStatus'], 
                            country=country_name)
                print(f"   - 노드 생성: {row['Org']} (Located in {country_name})")

            # 3) 기업 간 관계(Relationships) 생성
            st_map = self.subtypes.set_index('Subtype').to_dict('index')
            
            for _, row in self.relations.iterrows():
                subtype_info = st_map.get(row['Subtype'])
                if not subtype_info: continue

                # 관계 속성 준비
                rel_type = subtype_info['neo4j_edge_type']
                category = subtype_info['category']
                
                # 대칭 관계(A↔B)인 경우 ID 정렬로 중복 방지
                source, target = row['SourceOrg'], row['TargetOrg']
                if subtype_info['direction'] == 'A↔B':
                    source, target = sorted([source, target])

                # Cypher 쿼리: 관계 연결
                # 관계 타입(rel_type)은 동적이므로 f-string 사용
                rel_query = f"""
                MATCH (a:Organization {{orgId: $source}})
                MATCH (b:Organization {{orgId: $target}})
                MERGE (a)-[r:{rel_type}]->(b)
                SET r.subtype = $subtype,
                    r.category = $category,
                    r.evidence = $evidence,
                    r.updatedAt = datetime()
                """
                session.run(rel_query, source=source, target=target,
                            subtype=row['Subtype'], 
                            category=category,
                            evidence=row['Business_Relation_Sources'])
                print(f"   - 관계 연결: {source} -[{rel_type}]-> {target}")

            print("✅ 모든 데이터 적재가 완료되었습니다!")

# --- 실행부 ---
if __name__ == "__main__":
    loader = Neo4jLoader(URI, AUTH)
    try:
        loader.load_graph()
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    finally:
        loader.close()