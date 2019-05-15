plugins {
    kotlin("jvm") version "1.3.21"
    application
}

group = "com.canastic"
version = "0.1.0-alpha"

repositories {
    jcenter()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))

    compile(files("${System.getenv("INFORMIXDIR")}/jdbc/lib/ifxjdbc.jar"))

    implementation("de.huxhorn.sulky:de.huxhorn.sulky.ulid:8.2.0")
    implementation("com.beust:klaxon:5.0.1")
    implementation("io.javalin:javalin:2.8.0")
    implementation("org.jetbrains.kotlin:kotlin-reflect:1.3.31")

    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
}

application {
    // Define the main class for the application.
    mainClassName = "informixcdc.MainKt"
}
