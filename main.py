import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

# 1. .env 파일에서 설정 로드 (보안을 위해)
load_dotenv()

# .env 파일에서 주소와 비밀번호를 가져옵니다.
URI = os.getenv("NEO4J_URI")
AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

def main():
    # 2. 드라이버 연결 시도
    try:
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            # 연결 확인
            driver.verify_connectivity()
            print("✅ Neo4j Aura에 성공적으로 연결되었습니다!")

            # 3. 데이터 생성 (Create) - 예시: '철수'라는 사람 만들기
            driver.execute_query(
                "MERGE (p:Person {name: '철수'}) RETURN p"
            )
            print("👤 '철수' 노드가 생성(또는 확인)되었습니다.")

            # 4. 데이터 조회 (Read) - 데이터베이스에 있는 사람 이름 가져오기
            records, summary, keys = driver.execute_query(
                "MATCH (p:Person) RETURN p.name AS name"
            )
            
            print("📋 현재 데이터베이스에 있는 사람 목록:")
            for record in records:
                print(f"- {record['name']}")

    except Exception as e:
        print(f"❌ 연결 실패: {e}")
        print("💡 팁: .env 파일의 주소나 비밀번호가 맞는지 확인해보세요.")

if __name__ == "__main__":
    main()