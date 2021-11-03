# Contribution Guidelines

Thank you for your interest in contributing to the Axon Framework Kafka Extension. To make sure using Axon is a smooth
experience for everybody, we've set up a number of guidelines to follow.

There are different ways in which you can contribute to the framework:

1. You can report any bugs, feature requests or ideas about improvements on
   our [issue page](https://github.com/AxonFramework/extension-kafka/issues/new/choose). All ideas are welcome. Please
   be as exact as possible when reporting bugs. This will help us reproduce and thus solve the problem faster.
2. If you have created a component for your own application that you think might be useful to include in the framework,
   send us a pull request (or a patch / zip containing the source code). We will evaluate it and try to fit it in the
   framework. Please make sure code is properly documented using JavaDoc. This helps us to understand what is going on.
3. If you know of any other way you think you can help us, do not hesitate to send a message to
   the [AxonIQ's discussion platform](https://discuss.axoniq.io/).

## Code Contributions

If you're contributing code, please take care of the following:

### Contributor Licence Agreement

To keep everyone out of trouble (both you and us), we require that all contributors (digitally) sign a Contributor
License Agreement. Basically, the agreement says that we may freely use the code you contribute to the Axon Framework
Kafka Extension, and that we won't hold you liable for any unfortunate side effects that the code may cause.

To sign the CLA, visit: https://cla-assistant.io/AxonFramework/extension-kafka

### Code Style

We're trying very hard to maintain a consistent style of coding throughout the code base. Think of things like indenting
using 4 spaces, putting opening brackets (the '{') on the same line and putting proper JavaDoc on all non-private
members.

If you're using IntelliJ IDEA, you can download the code style
definition [here](https://github.com/AxonFramework/AxonFramework/blob/master/axon_code_style.xml). Simply import the XML
file in under "Settings -> Code Style -> Scheme -> Import Scheme". Doing so should make the code style selectable
immediately.

### Project Build

The project is built with Apache Maven, supplied by the Maven Wrapper `mvnw`. For separate aspects of the build Maven 
profiles are used.

For a **regular** build, execute from your command line: `./mvnw`. This will run the build and execute JUnit tests
of all modules and package the resulting artifacts. 

There is an example project supplied, and you can skip its build by passing `-DskipExamples` to your build command. 

The long-running integration tests (starting Spring Boot Application and/or running Kafka in a TestContainer) are 
provided and **ARE NOT** executed by default. A special **itest** build is needed to run those long-running tests. 
If you want to run them, please call `./mvnw -Pitest` from your command line. If you need to provide additional 
integration tests, make sure the class name ends with `IntegrationTest`.

The project uses JaCoCo to measure test coverage of the code and will automatically generate coverage reports on regular
and itest build. If you are interested in the overall test coverage, please run `./mvnw -Pcoverage-aggregate` 
(without calling `clean`) after your run the **regular** and **itest** builds and check the resulting aggregated report 
in `./coverage-report-generator/target/site/jacoco-aggregate/index.html`

