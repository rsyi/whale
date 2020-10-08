# Custom UI/UX

The majority of custom whale behavior can be achieved by modifying the `~/.whale/config/config.yaml` file, accessible through the command `wh config`.

## Changing the default editor

Whale references the `$EDITOR` environmental variable to determine what binary should be used to open the table stub markdown files when `enter` is pressed on a selected table in `wh`. To change the default editor, simply set this variable in your shell's `.rc` file.

## Modifying the preview command

By default, table stubs surfaced in `wh` are done so through `bat --color=always --style='changes'`, providing syntax highlighting and inline git change-tracking. If `bat` is not installed, `cat` is used as a fallback.

However, we also expose a config setting `preview_command`, which can be added to your `wh config` file in plain yaml as follows:

```text
preview_command: bat --color=always
```

This is primarily useful for customizing the flags passed to `bat` \(you can set the theme, the layout, etc.\), but any command that writes to `stdout` can also be used here.

