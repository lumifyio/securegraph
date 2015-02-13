package org.securegraph.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import jline.TerminalFactory;
import jline.UnixTerminal;
import jline.UnsupportedTerminal;
import jline.WindowsTerminal;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.tools.shell.AnsiDetector;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;
import org.codehaus.groovy.tools.shell.Interpreter;
import org.codehaus.groovy.tools.shell.util.Logger;
import org.codehaus.groovy.tools.shell.util.NoExitSecurityManager;
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.AnsiConsole;
import org.securegraph.Graph;
import org.securegraph.GraphFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.*;

public class SecuregraphShell {
    private final Groovysh groovysh;

    @Parameter(names = {"-C"}, description = "Suppress colors")
    private boolean suppressColor;

    @Parameter(names = {"-T"}, description = "Terminal type")
    private String terminalType = TerminalFactory.AUTO;

    @Parameter(names = {"-e"}, description = "String to evaluate")
    private String evalString = null;

    @Parameter(names = {"-c"}, description = "Configuration file name", required = true)
    private String configFileName = null;

    @Parameter(names = {"-cp"}, description = "Configuration property prefix")
    private String configPropertyPrefix = null;

    @Parameter(names = {"-a"}, description = "Authorizations")
    private String authorizations = null;

    @Parameter(description = "File names to execute")
    private List<String> fileNames = new ArrayList<>();

    public SecuregraphShell(String[] args) throws Exception {
        new JCommander(this, args);
        setTerminalType(terminalType, suppressColor);

        Map config = loadConfig();
        Graph graph = new GraphFactory().createGraph(config);

        System.setProperty("groovysh.prompt", "securegraph");

        // IO must be constructed AFTER calling setTerminalType()/AnsiConsole.systemInstall(),
        // else wrapped System.out does not work on Windows.
        IO io = new IO();

        Logger.io = io;

        CompilerConfiguration compilerConfiguration = new CompilerConfiguration();
        SecuregraphScript.setGraph(graph);
        if (authorizations != null) {
            SecuregraphScript.setAuthorizations(graph.createAuthorizations(authorizations.split(",")));
        }
        compilerConfiguration.setScriptBaseClass(SecuregraphScript.class.getName());

        Binding binding = new Binding();

        GroovyShell groovyShell = new GroovyShell(this.getClass().getClassLoader(), binding, compilerConfiguration);

        groovysh = new Groovysh(io);
        setGroovyShell(groovysh, groovyShell);

        startGroovysh(evalString, fileNames);
    }

    private Map loadConfig() throws IOException {
        File configFile = new File(configFileName);
        if (!configFile.exists()) {
            throw new RuntimeException("Could not load config file: " + configFile.getAbsolutePath());
        }

        Properties props = new Properties();
        try (InputStream in = new FileInputStream(configFile)) {
            props.load(in);
        }

        Map result = new HashMap();
        if (configPropertyPrefix == null) {
            result.putAll(props);
        } else {
            for (Map.Entry<Object, Object> p : props.entrySet()) {
                String key = (String) p.getKey();
                String val = (String) p.getValue();
                if (key.startsWith(configPropertyPrefix + ".")) {
                    result.put(key.substring((configPropertyPrefix + ".").length()), val);
                } else if (key.startsWith(configPropertyPrefix)) {
                    result.put(key.substring(configPropertyPrefix.length()), val);
                }
            }
        }

        return result;
    }

    private void setGroovyShell(Groovysh groovysh, GroovyShell groovyShell) throws NoSuchFieldException, IllegalAccessException {
        Field interpField = groovysh.getClass().getDeclaredField("interp");
        interpField.setAccessible(true);

        Field shellField = Interpreter.class.getDeclaredField("shell");
        shellField.setAccessible(true);

        Interpreter interpreter = (Interpreter) interpField.get(groovysh);
        shellField.set(interpreter, groovyShell);
    }

    public Groovysh getGroovysh() {
        return groovysh;
    }

    public static void main(final String[] args) throws Exception {
        new SecuregraphShell(args);
    }

    /**
     * @param evalString commands that will be executed at startup after loading files given with filenames param
     * @param filenames  files that will be loaded at startup
     */
    protected void startGroovysh(String evalString, List<String> filenames) throws IOException {
        int code;
        final Groovysh shell = getGroovysh();

        // Add a hook to display some status when shutting down...
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                //
                // FIXME: We need to configure JLine to catch CTRL-C for us... Use gshell-io's InputPipe
                //

                if (shell.getHistory() != null) {
                    try {
                        shell.getHistory().flush();
                    } catch (IOException e) {
                        System.out.println("Could not flush history.");
                    }
                }
            }
        });

        SecurityManager psm = System.getSecurityManager();
        System.setSecurityManager(new NoExitSecurityManager());

        try {
            code = shell.run(evalString, filenames);
        } finally {
            System.setSecurityManager(psm);
        }

        // Force the JVM to exit at this point, since shell could have created threads or
        // popped up Swing components that will cause the JVM to linger after we have been
        // asked to shutdown

        System.exit(code);
    }

    static void setTerminalType(String type, boolean suppressColor) {
        assert type != null;

        type = type.toLowerCase();
        boolean enableAnsi = true;
        switch (type) {
            case TerminalFactory.AUTO:
                type = null;
                break;
            case TerminalFactory.UNIX:
                type = UnixTerminal.class.getCanonicalName();
                break;
            case TerminalFactory.WIN:
            case TerminalFactory.WINDOWS:
                type = WindowsTerminal.class.getCanonicalName();
                break;
            case TerminalFactory.FALSE:
            case TerminalFactory.OFF:
            case TerminalFactory.NONE:
                type = UnsupportedTerminal.class.getCanonicalName();
                // Disable ANSI, for some reason UnsupportedTerminal reports ANSI as enabled, when it shouldn't
                enableAnsi = false;
                break;
            default:
                // Should never happen
                throw new IllegalArgumentException("Invalid Terminal type: $type");
        }
        if (enableAnsi) {
            installAnsi(); // must be called before IO(), since it modifies System.in
            Ansi.setEnabled(!suppressColor);
        } else {
            Ansi.setEnabled(false);
        }

        if (type != null) {
            System.setProperty(TerminalFactory.JLINE_TERMINAL, type);
        }
    }

    static void installAnsi() {
        // Install the system adapters, replaces System.out and System.err
        // Must be called before using IO(), because IO stores refs to System.out and System.err
        AnsiConsole.systemInstall();

        // Register jline ansi detector
        Ansi.setDetector(new AnsiDetector());
    }


    static void setSystemProperty(final String nameValue) {
        String name;
        String value;

        if (nameValue.indexOf('=') > 0) {
            String[] tmp = nameValue.split("=", 2);
            name = tmp[0];
            value = tmp[1];
        } else {
            name = nameValue;
            value = Boolean.TRUE.toString();
        }

        System.setProperty(name, value);
    }
}
