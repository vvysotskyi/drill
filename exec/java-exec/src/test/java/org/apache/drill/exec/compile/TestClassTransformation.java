/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.compile;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.bytecode.ValueHolderReplacementVisitor;
import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.exec.compile.ClassTransformer.ClassSet;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.options.SessionOptionManager;
import org.codehaus.commons.compiler.CompileException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.tree.ClassNode;

public class TestClassTransformation extends BaseTestQuery {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestClassTransformation.class);

  private static final int ITERATION_COUNT = Integer.parseInt(System.getProperty("TestClassTransformation.iteration", "1"));

  private static SessionOptionManager sessionOptions;

  @BeforeClass
  public static void beforeTestClassTransformation() {
    // Tests here require the byte-code merge technique and are meaningless
    // if the plain-old Java technique is selected. Force the plain-Java
    // technique to be off if it happened to be set on in the default
    // configuration.
    System.setProperty(CodeCompiler.PREFER_POJ_CONFIG, "false");
    final UserSession userSession = UserSession.Builder.newBuilder()
      .withOptionManager(getDrillbitContext().getOptionManager())
      .build();
    sessionOptions = userSession.getOptions();
  }

  @Test
  public void testJaninoClassCompiler() throws Exception {
    logger.debug("Testing JaninoClassCompiler");
    sessionOptions.setLocalOption(ClassCompilerSelector.JAVA_COMPILER_OPTION, ClassCompilerSelector.CompilerPolicy.JANINO.name());
    for (int i = 0; i < ITERATION_COUNT; i++) {
      compilationInnerClass(false); // Traditional byte-code manipulation
      compilationInnerClass(true); // Plain-old Java
    }
  }

  @Test
  public void testJDKClassCompiler() throws Exception {
    logger.debug("Testing JDKClassCompiler");
    sessionOptions.setLocalOption(ClassCompilerSelector.JAVA_COMPILER_OPTION, ClassCompilerSelector.CompilerPolicy.JDK.name());
    for (int i = 0; i < ITERATION_COUNT; i++) {
      compilationInnerClass(false); // Traditional byte-code manipulation
      compilationInnerClass(true); // Plain-old Java
    }
  }

  @Test
  public void testCompilationNoDebug() throws CompileException, ClassNotFoundException, ClassTransformationException, IOException {
    CodeGenerator<ExampleInner> cg = newCodeGenerator(ExampleInner.class, ExampleTemplateWithInner.class);
    ClassSet classSet = new ClassSet(null, cg.getDefinition().getTemplateClassName(), cg.getMaterializedClassName());
    String sourceCode = cg.generateAndGet();
    sessionOptions.setLocalOption(ClassCompilerSelector.JAVA_COMPILER_OPTION, ClassCompilerSelector.CompilerPolicy.JDK.name());
    sessionOptions.setLocalOption(ClassCompilerSelector.JAVA_COMPILER_DEBUG_OPTION, false);

    QueryClassLoader loader = new QueryClassLoader(config, sessionOptions);
    byte[][] codeWithoutDebug = loader.getClassByteCode(classSet.generated, sourceCode);
    loader.close();
    int sizeWithoutDebug = 0;
    for (byte[] bs : codeWithoutDebug) {
      sizeWithoutDebug += bs.length;
    }

    sessionOptions.setLocalOption(ClassCompilerSelector.JAVA_COMPILER_DEBUG_OPTION, true);
    loader = new QueryClassLoader(config, sessionOptions);
    byte[][] codeWithDebug = loader.getClassByteCode(classSet.generated, sourceCode);
    loader.close();
    int sizeWithDebug = 0;
    for (byte[] bs : codeWithDebug) {
      sizeWithDebug += bs.length;
    }

    Assert.assertTrue("Debug code is smaller than optimized code!!!", sizeWithDebug > sizeWithoutDebug);
    logger.debug("Optimized code is {}% smaller than debug code.", (int)((sizeWithDebug - sizeWithoutDebug)/(double)sizeWithDebug*100));
  }

  @Test // DRILL-6524
  public void testScalarReplacementInCondition() throws Exception {
    ClassTransformer.ClassNames classNames = new ClassTransformer.ClassNames("org.apache.drill.CompileClassWithIfs");
    String entireClass = DrillFileUtils.getResourceAsString(DrillFileUtils.SEPARATOR + classNames.slash + ".java");

    sessionOptions.setLocalOption(ClassCompilerSelector.JAVA_COMPILER_DEBUG_OPTION, false);

    List<String> compilers = Arrays.asList(ClassCompilerSelector.CompilerPolicy.JANINO.name(),
        ClassCompilerSelector.CompilerPolicy.JDK.name());
    for (String compilerName : compilers) {
      sessionOptions.setLocalOption(ClassCompilerSelector.JAVA_COMPILER_OPTION, compilerName);

      QueryClassLoader queryClassLoader = new QueryClassLoader(config, sessionOptions);

      byte[][] implementationClasses = queryClassLoader.getClassByteCode(classNames, entireClass);

      ClassNode originalClass = AsmUtil.classFromBytes(implementationClasses[0], ClassReader.EXPAND_FRAMES);

      ClassNode transformedClass = new ClassNode();
      DrillCheckClassAdapter mergeGenerator = new DrillCheckClassAdapter(CompilationConfig.ASM_API_VERSION,
          new CheckClassVisitorFsm(CompilationConfig.ASM_API_VERSION, transformedClass), true);
      originalClass.accept(new ValueHolderReplacementVisitor(mergeGenerator, true));

      if (!AsmUtil.isClassOk(logger, classNames.dot, transformedClass)) {
        throw new IllegalStateException(String.format("Problem found after transforming %s", classNames.dot));
      }
      ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
      transformedClass.accept(writer);
      byte[] outputClass = writer.toByteArray();

      queryClassLoader.injectByteCode(classNames.dot, outputClass);
      Class<?> transformedClazz = queryClassLoader.findClass(classNames.dot);
      transformedClazz.getMethod("doSomething").invoke(null);
    }
  }

  @Test
  @Ignore
  public void testScalarReplacementWithArrayAssignment() throws Exception {
    ClassTransformer.ClassNames classNames = new ClassTransformer.ClassNames("StreamingAggregatorGen4");
    String entireClass = "\n" +
        "\n" +
        "import org.apache.drill.exec.exception.SchemaChangeException;\n" +
        "import org.apache.drill.exec.expr.holders.BigIntHolder;\n" +
        "import org.apache.drill.exec.physical.impl.aggregate.InternalBatch;\n" +
        "import org.apache.drill.exec.record.RecordBatch;\n" +
        "import org.apache.drill.exec.vector.BigIntVector;\n" +
        "\n" +
        "public class StreamingAggregatorGen4 {\n" +
        "\n" +
        "  BigIntHolder work0;\n" +
        "  BigIntHolder const1;\n" +
        "  BigIntHolder constant2;\n" +
        "  BigIntVector vv4;\n" +
        "\n" +
        "  public void addRecord(int index)\n" +
        "      throws SchemaChangeException\n" +
        "  {\n" +
        "    {\n" +
        "      constant2 = new BigIntHolder();\n" +
        "      constant2 .value = const1 .value;\n" +
        "      BigIntHolder in = constant2;\n" +
        "      BigIntHolder value = work0;\n" +
        "        CountFunctions$BigIntCountFunction_add:\n" +
        "      {\n" +
        "        value.value++;\n" +
        "      }\n" +
        "      work0 = value;\n" +
        "    }\n" +
        "  }\n" +
        "\n" +
        "  public int getVectorIndex(int recordIndex)\n" +
        "      throws SchemaChangeException\n" +
        "  {\n" +
        "    {\n" +
        "      return (recordIndex);\n" +
        "    }\n" +
        "  }\n" +
        "\n" +
        "  public boolean isSame(int index1, int index2)\n" +
        "      throws SchemaChangeException\n" +
        "  {\n" +
        "    {\n" +
        "      return true;\n" +
        "    }\n" +
        "  }\n" +
        "\n" +
        "  public boolean isSamePrev(int b1Index, InternalBatch b1, int b2Index)\n" +
        "      throws SchemaChangeException\n" +
        "  {\n" +
        "    {\n" +
        "      return true;\n" +
        "    }\n" +
        "  }\n" +
        "\n" +
        "  public void outputRecordKeys(int inIndex, int outIndex)\n" +
        "      throws SchemaChangeException\n" +
        "  {\n" +
        "  }\n" +
        "\n" +
        "  public void outputRecordKeysPrev(InternalBatch previous, int previousIndex, int outIndex)\n" +
        "      throws SchemaChangeException\n" +
        "  {\n" +
        "  }\n" +
        "\n" +
        "  public void outputRecordValues(int outIndex)\n" +
        "      throws SchemaChangeException\n" +
        "  {\n" +
        "    {\n" +
        "      BigIntHolder out3;\n" +
        "      {\n" +
        "        final BigIntHolder out = new BigIntHolder();\n" +
        "        BigIntHolder value = work0;\n" +
        "          CountFunctions$BigIntCountFunction_output:\n" +
        "        {\n" +
        "          out.value = value.value;\n" +
        "        }\n" +
        "        work0 = value;\n" +
        "        out3 = out;\n" +
        "      }\n" +
        "      vv4 .getMutator().setSafe((outIndex), out3 .value);\n" +
        "    }\n" +
        "  }\n" +
        "\n" +
        "  public boolean resetValues()\n" +
        "      throws SchemaChangeException\n" +
        "  {\n" +
        "    {\n" +
        "      /** start RESET for function count **/\n" +
        "      {\n" +
        "        BigIntHolder value = work0;\n" +
        "          CountFunctions$BigIntCountFunction_reset:\n" +
        "        {\n" +
        "          value.value = 0;\n" +
        "        }\n" +
        "        work0 = value;\n" +
        "      }\n" +
        "      /** end RESET for function count **/\n" +
        "      return true;\n" +
        "    }\n" +
        "  }\n" +
        "\n" +
        "  public void setupInterior(RecordBatch incoming, RecordBatch outgoing)\n" +
        "      throws SchemaChangeException\n" +
        "  {\n" +
        "    {\n" +
        "      /** start SETUP for function count **/\n" +
        "      {\n" +
        "        BigIntHolder value = work0;\n" +
        "          CountFunctions$BigIntCountFunction_setup:\n" +
        "        {\n" +
        "          value = new BigIntHolder();\n" +
        "          value.value = 0;\n" +
        "        }\n" +
        "        work0 = value;\n" +
        "      }\n" +
        "      /** end SETUP for function count **/\n" +
        "      int[] fieldIds5 = new int[ 1 ] ;\n" +
        "      fieldIds5 [ 0 ] = 0;\n" +
        "      Object tmp6 = (outgoing).getValueAccessorById(BigIntVector.class, fieldIds5).getValueVector();\n" +
        "      if (tmp6 == null) {\n" +
        "        throw new SchemaChangeException(\"Failure while loading vector vv4 with id: TypedFieldId [fieldIds=[0], remainder=null].\");\n" +
        "      }\n" +
        "      vv4 = ((BigIntVector) tmp6);\n" +
        "    }\n" +
        "  }\n" +
        "\n" +
        "  public void __DRILL_INIT__()\n" +
        "      throws SchemaChangeException\n" +
        "  {\n" +
        "    {\n" +
        "      work0 = new BigIntHolder();\n" +
        "    }\n" +
        "  }\n" +
        "\n" +
        "}\n";

    sessionOptions.setLocalOption(ClassCompilerSelector.JAVA_COMPILER_DEBUG_OPTION, false);

    List<String> compilers = Arrays.asList(
        ClassCompilerSelector.CompilerPolicy.JANINO.name()
//        ,
//        ClassCompilerSelector.CompilerPolicy.JDK.name()
        );
    for (String compilerName : compilers) {
      sessionOptions.setLocalOption(ClassCompilerSelector.JAVA_COMPILER_OPTION, compilerName);

      QueryClassLoader queryClassLoader = new QueryClassLoader(config, sessionOptions);

      byte[][] implementationClasses = queryClassLoader.getClassByteCode(classNames, entireClass);

//      Files.write(implementationClasses[0], new File("strclass.class"));

      ClassNode originalClass = AsmUtil.classFromBytes(implementationClasses[0], ClassReader.EXPAND_FRAMES);

//      ClassNode transformedClass = new ClassNode();
//      DrillCheckClassAdapter mergeGenerator = new DrillCheckClassAdapter(CompilationConfig.ASM_API_VERSION,
//          new CheckClassVisitorFsm(CompilationConfig.ASM_API_VERSION, transformedClass), true);
//      originalClass.accept(new ValueHolderReplacementVisitor(mergeGenerator, true));
//
//      if (!AsmUtil.isClassOk(logger, classNames.dot, transformedClass)) {
//        throw new IllegalStateException(String.format("Problem found after transforming %s", classNames.dot));
//      }
//      ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
//      transformedClass.accept(writer);
//      byte[] outputClass = writer.toByteArray();
//
//      queryClassLoader.injectByteCode(classNames.dot, outputClass);
//      Class<?> transformedClazz = queryClassLoader.findClass(classNames.dot);
//      transformedClazz.getMethod("doSomething").invoke(null);
    }
  }

  @Test // DRILL-5683
  public void testCaseWithColumnExprsOnView() throws Exception {
    String sqlCreate =
      "create table dfs.tmp.t1 as\n" +
          "select r_regionkey, r_name, case when mod(r_regionkey, 3) > 0 then mod(r_regionkey, 3) else null end as flag\n" +
          "from cp.`tpch/region.parquet`";
    try {
      test(sqlCreate);
      String sql = "select * from dfs.tmp.t1 where NOT (flag IS NOT NULL)";

      setSessionOption(ExecConstants.SCALAR_REPLACEMENT_OPTION, ClassTransformer.ScalarReplacementOption.ON.name());

      List<String> compilers = Arrays.asList(ClassCompilerSelector.CompilerPolicy.JANINO.name(),
          ClassCompilerSelector.CompilerPolicy.JDK.name());

      for (String compilerName : compilers) {
        setSessionOption(ClassCompilerSelector.JAVA_COMPILER_OPTION, compilerName);

        testBuilder()
            .sqlQuery(sql)
            .unOrdered()
            .baselineColumns("r_regionkey", "r_name", "flag")
            .baselineValues(0, "AFRICA", null)
            .baselineValues(3, "EUROPE", null)
            .go();
      }
    } finally {
      test("drop table if exists dfs.tmp.t1");
      resetSessionOption(ExecConstants.SCALAR_REPLACEMENT_OPTION);
      resetSessionOption(ClassCompilerSelector.JAVA_COMPILER_OPTION);
    }
  }

  /**
   * Do a test of a three level class to ensure that nested code generators works correctly.
   */
  private void compilationInnerClass(boolean asPoj) throws Exception{
    CodeGenerator<ExampleInner> cg = newCodeGenerator(ExampleInner.class, ExampleTemplateWithInner.class);
    cg.preferPlainJava(asPoj);

    CodeCompiler.CodeGenCompiler cc = new CodeCompiler.CodeGenCompiler(config, sessionOptions);
    @SuppressWarnings("unchecked")
    Class<? extends ExampleInner> c = (Class<? extends ExampleInner>) cc.generateAndCompile(cg);
    ExampleInner t = c.newInstance();
    t.doOutside();
    t.doInsideOutside();
  }

  private <T, X extends T> CodeGenerator<T> newCodeGenerator(Class<T> iface, Class<X> impl) {
    final TemplateClassDefinition<T> template = new TemplateClassDefinition<>(iface, impl);
    CodeGenerator<T> cg = CodeGenerator.get(template, getDrillbitContext().getOptionManager());
    cg.plainJavaCapable(true);

    ClassGenerator<T> root = cg.getRoot();
    root.setMappingSet(new MappingSet(new GeneratorMapping("doOutside", null, null, null)));
    root.getSetupBlock().directStatement("System.out.println(\"outside\");");


    ClassGenerator<T> inner = root.getInnerGenerator("TheInnerClass");
    inner.setMappingSet(new MappingSet(new GeneratorMapping("doInside", null, null, null)));
    inner.getSetupBlock().directStatement("System.out.println(\"inside\");");

    ClassGenerator<T> doubleInner = inner.getInnerGenerator("DoubleInner");
    doubleInner.setMappingSet(new MappingSet(new GeneratorMapping("doDouble", null, null, null)));
    doubleInner.getSetupBlock().directStatement("System.out.println(\"double\");");
    return cg;
  }
}
