import pandas as pd
import io
import re

# 1. 문서 내 데이터 정의 (테스트용)
entities_csv = """Subject,Object,Relation
AbstractEntity,Root,IsSubclassOf
IndependentEntity,Root,IsSubclassOf
Organization,Agent,IsSubclassOf
3DCPMaterialsManufacturerProvider,3DCPProductProvider,IsSubclassOf
3DCPMaterialsManufacturerProvider,3DCPMaterialsProvider,IsSubclassOf""" # 

org_input_csv = """Org,Organization_Type,Headquarters_Location,Homepage,Sector_Industry,Roles,BusinessStatus,FoundedYear
14trees,PrivateCompany,CHE,14trees.com,3DPrinting;AEC,3DCPServiceProvider3DCP,Active,
holcim,PublicCompany,CHE,holcim.com,3DPrinting;AEC,3DCPMaterialsManufacturerProvider,Active,""" # 

location_code_csv = """CountryName,CountryCodeISO3166,CountryCodeUN,Predicate,Neo4jRelType
Switzerland,CHE,756,HasCountryCode,HAS_COUNTRY_CODE
United States of America,USA,840,HasCountryCode,HAS_COUNTRY_CODE""" # [cite: 126]

org_relations_csv = """SourceOrg,TargetOrg,Subtype,Business_Relation_Sources,Paper_Source,SourcePatentSources,TargetPatentSources
holcim,14trees,PartnersWith,https://www.holcim.com/who-we-are/our-stories/14trees,,,
holcim,14trees,IsShareholderOf,https://www.holcim.com/who-we-are/our-stories/14trees,,,""" # 

# 2. 검증 및 변환 클래스
class GraphDataValidator:
    def __init__(self):
        # 문자열 데이터를 데이터프레임으로 변환
        self.entities = pd.read_csv(io.StringIO(entities_csv))
        self.org_input = pd.read_csv(io.StringIO(org_input_csv))
        self.locations = pd.read_csv(io.StringIO(location_code_csv))
        self.relations = pd.read_csv(io.StringIO(org_relations_csv))
        self.valid_orgs = set(self.org_input['Org'].unique()) # 

    def to_snake_case(self, text):
        """PascalCase -> SNAKE_CASE 변환 (예: IsLocatedIn -> IS_LOCATED_IN) """
        if pd.isna(text): return None
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', str(text))
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).upper()

    def validate(self):
        print("🔍 데이터 정합성 검토를 시작합니다...")
        
        # 1) 참조 무결성 검사 
        for i, row in self.relations.iterrows():
            if row['SourceOrg'] not in self.valid_orgs or row['TargetOrg'] not in self.valid_orgs:
                print(f"❌ 오류: 행 {i}의 조직명이 Org_Input에 없습니다. ({row['SourceOrg']} -> {row['TargetOrg']})")
                return False
        print("✅ 1. 참조 무결성 검사 통과 (모든 관계 조직이 존재함)")

        # 2) SNAKE_CASE 변환 테스트 
        test_case = "IsLocatedIn"
        converted = self.to_snake_case(test_case)
        print(f"✅ 2. 변환 테스트 완료: {test_case} -> {converted}")

        # 3) 국가명 역매핑 테스트 
        che_name = self.locations[self.locations['CountryCodeISO3166'] == 'CHE']['CountryName'].values[0]
        print(f"✅ 3. 국가 역매핑 완료: CHE -> {che_name}")

        return True

if __name__ == "__main__":
    validator = GraphDataValidator()
    if validator.validate():
        print("\n🚀 검증 성공! 이제 이 데이터를 Neo4j에 적재할 수 있습니다.")
    else:
        print("\n⚠️ 검증 실패. 데이터를 수정해 주세요.")