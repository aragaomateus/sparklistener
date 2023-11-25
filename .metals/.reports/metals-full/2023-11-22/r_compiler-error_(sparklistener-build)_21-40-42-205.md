file://<WORKSPACE>/build.sbt
### file%3A%2F%2F%2FUsers%2Fmateusaragao%2Fsparklistener%2Fbuild.sbt:10: error: unclosed string literal
scalaVersion := "2.12
                ^

occurred in the presentation compiler.

action parameters:
uri: file://<WORKSPACE>/build.sbt
text:
```scala
import _root_.scala.xml.{TopScope=>$scope}
import _root_.sbt._
import _root_.sbt.Keys._
import _root_.sbt.nio.Keys._
import _root_.sbt.ScriptedPlugin.autoImport._, _root_.sbt.plugins.JUnitXmlReportPlugin.autoImport._, _root_.sbt.plugins.MiniDependencyTreePlugin.autoImport._, _root_.bloop.integrations.sbt.BloopPlugin.autoImport._
import _root_.sbt.plugins.IvyPlugin, _root_.sbt.plugins.JvmPlugin, _root_.sbt.plugins.CorePlugin, _root_.sbt.ScriptedPlugin, _root_.sbt.plugins.SbtPlugin, _root_.sbt.plugins.SemanticdbPlugin, _root_.sbt.plugins.JUnitXmlReportPlugin, _root_.sbt.plugins.Giter8TemplatePlugin, _root_.sbt.plugins.MiniDependencyTreePlugin, _root_.bloop.integrations.sbt.BloopPlugin

// The simplest possible sbt build file is just one line:

scalaVersion := "2.12
// That is, to create a valid sbt build, all you've got to do is define the
// version of Scala you'd like your project to use.

// ============================================================================

// Lines like the above defining `scalaVersion` are called "settings". Settings
// are key/value pairs. In the case of `scalaVersion`, the key is "scalaVersion"
// and the value is "2.13.8"

// It's possible to define many kinds of settings, such as:x

// Note, it's not required for you to define these three settings. These are
// mostly only necessary if you intend to publish your library's binaries on a
// place like Sonatype.


// Want to use a published library in your project?
// You can define other libraries as dependencies in your build like this:

lazy val root = project
  .in(file("."))
  .settings(
    name := "sparkListener",
    version := "0.1.0",

    libraryDependencies  ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.0",
      "org.apache.spark" %% "spark-sql" % "3.3.0",
      "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
      "org.apache.logging.log4j" % "log4j-api" % "2.14.1",
      "org.apache.logging.log4j" % "log4j-core" % "2.14.1")
  )


// Here, `libraryDependencies` is a set of dependencies, and by using `+=`,
// we're adding the scala-parser-combinators dependency to the set of dependencies
// that sbt will go and fetch when it starts up.
// Now, in any Scala file, you can import classes, objects, etc., from
// scala-parser-combinators with a regular import.

// TIP: To find the "dependency" that you need to add to the
// `libraryDependencies` set, which in the above example looks like this:

// "org.scala-lang.modules" %% "scala-parser-combinators" % "2.1.1"

// You can use Scaladex, an index of all known published Scala libraries. There,
// after you find the library you want, you can just copy/paste the dependency
// information that you need into your build file. For example, on the
// scala/scala-parser-combinators Scaladex page,
// https://index.scala-lang.org/scala/scala-parser-combinators, you can copy/paste
// the sbt dependency from the sbt box on the right-hand side of the screen.

// IMPORTANT NOTE: while build files look _kind of_ like regular Scala, it's
// important to note that syntax in *.sbt files doesn't always behave like
// regular Scala. For example, notice in this build file that it's not required
// to put our settings into an enclosing object or class. Always remember that
// sbt is a bit different, semantically, than vanilla Scala.

// ============================================================================

// Most moderately interesting Scala projects don't make use of the very simple
// build file style (called "bare style") used in this build.sbt file. Most
// intermediate Scala projects make use of so-called "multi-project" builds. A
// multi-project build makes it possible to have different folders which sbt can
// be configured differently for. That is, you may wish to have different
// dependencies or different testing frameworks defined for different parts of
// your codebase. Multi-project builds make this possible.

// Here's a quick glimpse of what a multi-project build looks like for this
// build, with only one "subproject" defined, called `root`:

// lazy val root = (project in file(".")).
//   settings(
//     inThisBuild(List(
//       organization := "ch.epfl.scala",
//       scalaVersion := "2.13.8"
//     )),
//     name := "hello-world"
//   )

// To learn more about multi-project builds, head over to the official sbt
// documentation at http://www.scala-sbt.org/documentation.html

```



#### Error stacktrace:

```
scala.meta.internal.tokenizers.Reporter.syntaxError(Reporter.scala:23)
	scala.meta.internal.tokenizers.Reporter.syntaxError$(Reporter.scala:23)
	scala.meta.internal.tokenizers.Reporter$$anon$1.syntaxError(Reporter.scala:33)
	scala.meta.internal.tokenizers.Reporter.syntaxError(Reporter.scala:25)
	scala.meta.internal.tokenizers.Reporter.syntaxError$(Reporter.scala:25)
	scala.meta.internal.tokenizers.Reporter$$anon$1.syntaxError(Reporter.scala:33)
	scala.meta.internal.tokenizers.LegacyScanner.getStringLit(LegacyScanner.scala:553)
	scala.meta.internal.tokenizers.LegacyScanner.fetchDoubleQuote$1(LegacyScanner.scala:372)
	scala.meta.internal.tokenizers.LegacyScanner.fetchToken(LegacyScanner.scala:376)
	scala.meta.internal.tokenizers.LegacyScanner.nextToken(LegacyScanner.scala:211)
	scala.meta.internal.tokenizers.LegacyScanner.foreach(LegacyScanner.scala:1011)
	scala.meta.internal.tokenizers.ScalametaTokenizer.uncachedTokenize(ScalametaTokenizer.scala:24)
	scala.meta.internal.tokenizers.ScalametaTokenizer.$anonfun$tokenize$1(ScalametaTokenizer.scala:17)
	scala.collection.concurrent.TrieMap.getOrElseUpdate(TrieMap.scala:895)
	scala.meta.internal.tokenizers.ScalametaTokenizer.tokenize(ScalametaTokenizer.scala:17)
	scala.meta.internal.tokenizers.ScalametaTokenizer$$anon$2.apply(ScalametaTokenizer.scala:332)
	scala.meta.tokenizers.Api$XtensionTokenizeDialectInput.tokenize(Api.scala:25)
	scala.meta.tokenizers.Api$XtensionTokenizeInputLike.tokenize(Api.scala:14)
	scala.meta.internal.parsers.ScannerTokens$.apply(ScannerTokens.scala:912)
	scala.meta.internal.parsers.ScalametaParser.<init>(ScalametaParser.scala:33)
	scala.meta.parsers.Parse$$anon$1.apply(Parse.scala:35)
	scala.meta.parsers.Api$XtensionParseDialectInput.parse(Api.scala:25)
	scala.meta.internal.semanticdb.scalac.ParseOps$XtensionCompilationUnitSource.toSource(ParseOps.scala:17)
	scala.meta.internal.semanticdb.scalac.TextDocumentOps$XtensionCompilationUnitDocument.toTextDocument(TextDocumentOps.scala:206)
	scala.meta.internal.pc.SemanticdbTextDocumentProvider.textDocument(SemanticdbTextDocumentProvider.scala:54)
	scala.meta.internal.pc.ScalaPresentationCompiler.$anonfun$semanticdbTextDocument$1(ScalaPresentationCompiler.scala:356)
```
#### Short summary: 

file%3A%2F%2F%2FUsers%2Fmateusaragao%2Fsparklistener%2Fbuild.sbt:10: error: unclosed string literal
scalaVersion := "2.12
                ^