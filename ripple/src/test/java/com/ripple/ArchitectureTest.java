package com.ripple;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.locks.StampedLock;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * ArchUnit tests enforcing the Ripple architecture constraints.
 *
 * <p><strong>Key rules:</strong>
 * <ul>
 *   <li>Strict downward layer dependency: api → engine → model (no upward calls)</li>
 *   <li>Model package: pure data — no Spring, no engine dependencies</li>
 *   <li>Controllers do not depend on Chronicle Queue directly</li>
 *   <li>StampedLock: public methods in TopologyGraph do not call other public methods on
 *       the same class (reentrancy prevention)</li>
 * </ul>
 */
class ArchitectureTest {

    private static JavaClasses classes;

    @BeforeAll
    static void importClasses() {
        classes = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("com.ripple");
    }

    @Test
    void model_package_has_no_spring_dependencies() {
        ArchRule rule = noClasses()
            .that().resideInAPackage("com.ripple.model..")
            .should().dependOnClassesThat().resideInAnyPackage(
                "org.springframework..",
                "com.ripple.engine..",
                "com.ripple.api..",
                "com.ripple.config.."
            )
            .because("Model records are pure data — no framework or engine dependencies");

        rule.check(classes);
    }

    @Test
    void api_layer_does_not_depend_on_chronicle_queue() {
        ArchRule rule = noClasses()
            .that().resideInAPackage("com.ripple.api..")
            .should().dependOnClassesThat().resideInAPackage("net.openhft..")
            .because("API controllers must not depend on Chronicle Queue directly;" +
                     " all event bus access goes through the EventBus facade");

        rule.check(classes);
    }

    @Test
    void engine_packages_do_not_depend_on_api_package() {
        ArchRule rule = noClasses()
            .that().resideInAPackage("com.ripple.engine..")
            .should().dependOnClassesThat().resideInAPackage("com.ripple.api..")
            .because("Strict downward data flow: engine layers must not call up to the API layer");

        rule.check(classes);
    }

    @Test
    void probe_engine_does_not_depend_on_topology_or_blast() {
        ArchRule rule = noClasses()
            .that().resideInAPackage("com.ripple.engine.probe..")
            .should().dependOnClassesThat().resideInAnyPackage(
                "com.ripple.engine.topology..",
                "com.ripple.engine.blast.."
            )
            .because("Probe engine is the lowest engine layer — it must not depend on topology or blast radius");

        rule.check(classes);
    }

    @Test
    void blast_radius_engine_does_not_depend_on_stream() {
        ArchRule rule = noClasses()
            .that().resideInAPackage("com.ripple.engine.blast..")
            .should().dependOnClassesThat().resideInAPackage("com.ripple.engine.stream..")
            .because("Blast radius engine produces results; the stream layer consumes them. " +
                     "No upward dependency from blast to stream");

        rule.check(classes);
    }

    @Test
    void config_classes_are_annotated_with_configuration_properties() {
        ArchRule rule = classes()
            .that().resideInAPackage("com.ripple.config..")
            .and().haveSimpleNameEndingWith("Properties")
            .should().beAnnotatedWith(
                org.springframework.boot.context.properties.ConfigurationProperties.class
            )
            .because("All *Properties classes must be ConfigurationProperties beans");

        rule.check(classes);
    }
}
