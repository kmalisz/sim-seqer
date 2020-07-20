/*
 * This file holds several functions used to render the nf-core ANSI header.
 */

class Headers {

    private static Map log_colours(Boolean monochrome_logs) {
        Map colorcodes = [:]
        colorcodes['reset'] = monochrome_logs ? '' : "\033[0m"
        colorcodes['dim'] = monochrome_logs ? '' : "\033[2m"
        colorcodes['black'] = monochrome_logs ? '' : "\033[0;30m"
        colorcodes['green'] = monochrome_logs ? '' : "\033[0;32m"
        colorcodes['yellow'] = monochrome_logs ? '' :  "\033[0;33m"
        colorcodes['yellow_bold'] = monochrome_logs ? '' : "\033[1;93m"
        colorcodes['blue'] = monochrome_logs ? '' : "\033[0;34m"
        colorcodes['purple'] = monochrome_logs ? '' : "\033[0;35m"
        colorcodes['cyan'] = monochrome_logs ? '' : "\033[0;36m"
        colorcodes['white'] = monochrome_logs ? '' : "\033[0;37m"
        colorcodes['red'] = monochrome_logs ? '' : "\033[1;91m"
        return colorcodes
    }

    static String nf_core(workflow, monochrome_logs) {
        Map colors = log_colours(monochrome_logs)
        String.format(
            """\n
            -${colors.dim}----------------------------------------------------${colors.reset}-
            ${colors.purple}     _   _____  ____   _____   ____     _          ${colors.reset}
            ${colors.purple}    / \\ |_   _||  _ \\ | ____| / ___|   / \\      ${colors.reset}
            ${colors.purple}   / _ \\  | |  | |_) ||  _|  | |      / _ \\      ${colors.reset}
            ${colors.purple}  / ___ \\ | |  |  _ < | |___ | |___  / ___ \\     ${colors.reset}
            ${colors.purple} /_/   \\_\\|_|  |_| \\_\\|_____| \\____|/_/   \\_\\ ${colors.reset}

            ${colors.green}  ${workflow.manifest.name} v${workflow.manifest.version}${colors.reset}
            -${colors.dim}----------------------------------------------------${colors.reset}-
            """.stripIndent()
        )
    }
}

