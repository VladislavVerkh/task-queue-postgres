import com.diffplug.gradle.spotless.SpotlessExtension
import org.gradle.api.plugins.JavaPlugin

plugins {
    id("org.springframework.boot") version "4.0.3" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
    id("com.diffplug.spotless") version "7.2.1" apply false
}

group = "dev.verkhovskiy"
version = "0.0.1-SNAPSHOT"

allprojects {
    repositories {
        mavenCentral()
    }
}

subprojects {
    group = rootProject.group
    version = rootProject.version

    plugins.withType<JavaPlugin> {
        apply(plugin = "com.diffplug.spotless")

        extensions.configure<SpotlessExtension> {
            java {
                target("src/*/java/**/*.java")
                googleJavaFormat("1.27.0")
            }
        }

        tasks.named("check") {
            dependsOn("spotlessCheck")
        }
    }
}
