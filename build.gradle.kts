plugins {
    kotlin("jvm") version "1.3.21"
}

group = "com.canastic"
version = "0.1.0-alpha"

repositories {
    jcenter()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))

    compile(files("${System.getenv("INFORMIXDIR")}/jdbc/lib/ifxjdbc.jar"))

    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
}
