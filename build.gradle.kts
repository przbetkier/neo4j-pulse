plugins {
    id("application")
    alias(libs.plugins.kotlinJvm)
}

group = "dev.przbetkier"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.neo4j.driver)
    implementation(libs.bundles.jackson)
    implementation(libs.bundles.ktor)
    implementation(libs.snakeyaml)
    implementation(libs.slf4j.simple)

    testImplementation(kotlin("test"))
}

application {
    mainClass = "dev.przbetkier.Neo4jPulseRunner"
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(23)
}

tasks.withType<Jar> {
    manifest {
        attributes["Main-Class"] = "dev.przbetkier.Neo4jPulseRunner"
    }
}