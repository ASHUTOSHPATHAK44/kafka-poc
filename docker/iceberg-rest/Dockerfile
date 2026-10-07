# =============================================================================
# Iceberg REST catalog image with the PostgreSQL JDBC driver added.
#
# The stock apache/iceberg-rest-fixture image ships only the SQLite driver, so
# pointing CATALOG_URI at Postgres fails with "No suitable driver found".
# The app is a single fat jar launched via `java -jar`, whose classpath is
# fixed in its manifest — a loose jar wouldn't be picked up. So we instead
# launch with an explicit -cp that includes BOTH the app jar and the driver.
# =============================================================================
ARG ICEBERG_VERSION=1.10.1
FROM apache/iceberg-rest-fixture:${ICEBERG_VERSION}

ARG POSTGRES_JDBC_VERSION=42.7.4

# Fetch the PostgreSQL JDBC driver into the app's working dir.
# ADD-from-URL creates the file as root with 0600, which the image's non-root
# runtime user cannot read (-> ClassNotFoundException). Make it world-readable.
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    /usr/lib/iceberg-rest/postgresql.jar

WORKDIR /usr/lib/iceberg-rest

# Launch with an explicit classpath (app jar + driver) instead of `java -jar`,
# so the Postgres driver is on the classpath and DriverManager can find it.
ENTRYPOINT ["java", "-cp", "iceberg-rest-adapter.jar:postgresql.jar", \
            "org.apache.iceberg.rest.RESTCatalogServer"]
CMD []
