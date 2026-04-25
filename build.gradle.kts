import com.diffplug.gradle.spotless.SpotlessExtension
import org.gradle.api.plugins.JavaPlugin
import org.gradle.testing.jacoco.tasks.JacocoReport

plugins {
    id("org.springframework.boot") version "4.0.3" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
    id("com.diffplug.spotless") version "7.2.1" apply false
    id("com.github.spotbugs") version "6.5.1" apply false
    id("net.ltgt.errorprone") version "5.1.0" apply false
    id("org.owasp.dependencycheck") version "12.2.1"
    id("org.cyclonedx.bom") version "3.2.4"
}

group = "dev.verkhovskiy"
version = "0.0.1-SNAPSHOT"

allprojects {
    repositories {
        mavenCentral()
    }
}

dependencyCheck {
    formats = listOf("HTML", "JSON")
    failBuildOnCVSS = 7.0f
    analyzers.assemblyEnabled = false
}

tasks.named("cyclonedxBom") {
    mustRunAfter("dependencyCheckAggregate")
}

tasks.register("securityCheck") {
    group = "verification"
    description = "Runs dependency vulnerability scanning and generates a CycloneDX SBOM."
    dependsOn("dependencyCheckAggregate", "cyclonedxBom")
}

subprojects {
    group = rootProject.group
    version = rootProject.version

    plugins.withType<JavaPlugin> {
        apply(plugin = "com.diffplug.spotless")
        apply(plugin = "com.github.spotbugs")
        apply(plugin = "jacoco")
        apply(plugin = "net.ltgt.errorprone")

        dependencies.add("compileOnly", "com.github.spotbugs:spotbugs-annotations:4.9.8")
        dependencies.add("errorprone", "com.google.errorprone:error_prone_core:2.49.0")

        extensions.configure<SpotlessExtension> {
            java {
                target("src/*/java/**/*.java")
                googleJavaFormat("1.27.0")
            }
        }

        tasks.withType<Test>().configureEach {
            useJUnitPlatform()
            finalizedBy("jacocoTestReport")
        }

        tasks.named<JacocoReport>("jacocoTestReport") {
            dependsOn(tasks.withType<Test>())
            reports {
                xml.required.set(true)
                html.required.set(true)
            }
        }

        tasks.named("check") {
            dependsOn("spotlessCheck", "jacocoTestReport")
        }
    }
}
