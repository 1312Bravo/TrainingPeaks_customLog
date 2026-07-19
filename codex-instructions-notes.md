# Notes About Codex Instructions, AGENTS.md, SKILL.md, Skills, And skills.sh

Last updated: 2026-07-05

## Short Version

`AGENTS.md` and `SKILL.md` are both instruction files, but they solve different problems.

- `AGENTS.md` is for project-local rules.
- `SKILL.md` is for reusable task workflows or reusable style guidance.
- A skill is usually a folder that contains a `SKILL.md` file, plus optional references, scripts, or assets.
- Plugins are bigger installable bundles that can contain skills, app integrations, MCP servers, and other resources.
- `skills.sh` is a public directory/leaderboard for discovering and installing community skills.

For my own workflow:

- Use `AGENTS.md` to tell Codex how to behave in this specific repository.
- Use `SKILL.md` to capture reusable preferences, such as my data science notebook style, dataframe formatting, plotting style, and collaboration preferences.
- Use a sync script when I want to copy the current instruction files into another project.

## Mental Model

Think of the files like this:

- Prompt in the current chat: what I want right now.
- `AGENTS.md`: how Codex should work in this repo or folder.
- `SKILL.md`: reusable workflow or style guidance Codex can apply when the task matches.
- Plugin: packaged skills plus optional tools or integrations.
- MCP server or connector: a live bridge to external data or actions.

The current chat still matters most for the task. If I ask for something specific, that request should guide the work. The persistent files are background guidance so Codex does not need to relearn my preferences every time.

## AGENTS.md

`AGENTS.md` is the best place for durable project instructions.

Good things to put in `AGENTS.md`:

- How to run tests in this repo.
- Where important files live.
- Data handling rules for this repo.
- Project-specific naming conventions.
- Which folders are source data and should be treated as read-only.
- Verification steps expected before finishing.
- Local sync or maintenance workflow for this instruction repository.

Bad things to put in `AGENTS.md`:

- Long reusable style guides that should apply everywhere.
- Generic instructions that are not really specific to this repo.
- Large skill-like workflows that would be easier to reuse as `SKILL.md`.

Codex discovers `AGENTS.md` files as project instructions. It can combine global instructions from the Codex home directory with project instructions from the repository. More specific nested files can override broader ones because they are loaded later in the instruction chain.

Practical rule:

- If the instruction is about this repository, put it in `AGENTS.md`.
- If the instruction is about how I generally like Codex to work across projects, put it in a skill.

## SKILL.md

`SKILL.md` is the core file inside a skill.

A skill is a reusable package of instructions. It can also include:

- scripts
- reference files
- templates
- assets
- examples

Skills are useful when the same workflow or style should be reused in many projects. In my case, the data science style guide belongs well in `SKILL.md` because I want Codex to use similar notebook, dataframe, modeling, plotting, and communication habits across projects.

Good things to put in `SKILL.md`:

- My reusable Python notebook style.
- My dataframe formatting conventions.
- My plotting formatting conventions.
- My preferred model diagnostic workflow.
- My collaboration preference that Codex should challenge my ideas when needed.
- Step-by-step workflows for recurring tasks.
- References or helper scripts that support a workflow.

Bad things to put in `SKILL.md`:

- One-off instructions for a single chat.
- Repo-specific commands that are not useful elsewhere.
- Sensitive local details that should not travel with a reusable skill.
- Huge unrelated collections of rules that make the skill trigger too broadly.

## How Codex Uses Skills

Codex can use skills in two main ways:

- Explicitly: I mention the skill directly, for example with `$skill-name`.
- Implicitly: Codex sees that the task matches the skill description and chooses to load it.

The description matters a lot. Codex initially sees a lightweight list of available skills with names, descriptions, and paths. It reads the full `SKILL.md` only after it decides that a skill is relevant.

This is called progressive disclosure. It keeps the context smaller while still allowing detailed instructions when needed.

Practical consequence:

- A skill description should clearly say when the skill should trigger.
- A vague skill description makes it less likely that Codex will use it correctly.
- A skill should be focused enough that using it is obviously helpful.

## Where Skills Can Live

Codex can discover skills from several places.

Common scopes:

- Repository skills: checked into a project, usually under `.agents/skills`.
- User skills: stored in a user-level skills folder so they apply across projects.
- Admin/system skills: installed globally or bundled with Codex.
- Plugin skills: installed as part of a plugin bundle.

For my own current setup, the simple model is:

- `Codex-Instructions/SKILL.md` is my reusable source file.
- I can sync that file into specific projects when I want the project to carry the same guidance.
- If I later want a more official reusable skill, I should place it in the proper `.agents/skills/<skill-name>/SKILL.md` structure.

Important distinction:

- A standalone `SKILL.md` in a repo is useful as documentation and can be synced.
- A formal Codex skill is normally a skill directory containing `SKILL.md`, often inside `.agents/skills`.

## Plugins

Plugins are larger than skills.

A plugin can include:

- one or more skills
- app integrations
- MCP server configuration
- assets
- marketplace metadata
- setup or authorization behavior

Use a skill when the main thing is reusable instructions or a workflow.

Use a plugin when I want to distribute something installable, bundle multiple skills together, or include integrations/tools beyond plain instructions.

Practical examples:

- My dataframe and plotting style: skill.
- A UTMB-specific data workflow with scripts and references: skill, maybe later plugin.
- A bundle that includes UTMB skills plus API tooling or MCP setup: plugin.

## skills.sh

`skills.sh` is a public directory and leaderboard for agent skills. It describes skills as reusable capabilities that give AI agents procedural knowledge. It supports many coding agents, including Codex.

What it is useful for:

- Discovering existing skills.
- Seeing popular skill repositories.
- Installing community skills through the `skills` CLI.
- Understanding the broader ecosystem around agent skills.

The basic install pattern shown by the site is:

```bash
npx skills add <owner/repo>
```

Example:

```bash
npx skills add vercel-labs/agent-skills
```

The site also has a leaderboard. The ranking is based on anonymous install telemetry from the `skills` CLI. The site says the telemetry is aggregate install data and not personal usage data.

Important caution:

- `skills.sh` is not the same thing as official Codex documentation.
- Community skills can be useful, but they should be reviewed before installing.
- Popular does not automatically mean safe, correct, or suitable for my workflow.
- A skill can influence how an agent thinks and acts, so installing random skills should be treated like adding a dependency.

## skills.sh Repository Pages

For repositories listed on `skills.sh`, maintainers can add a `skills.sh.json` file to control how the repository appears on the site.

This file can group skills into sections like:

- React
- Design
- Security
- Data
- Testing

Important detail:

- `skills.sh.json` only changes how the repository page appears on `skills.sh`.
- It does not change the actual skill content.
- It does not change how Codex reads `SKILL.md`.
- It does not change how the `skills` CLI installs a skill.

So `skills.sh.json` is display metadata, not instruction behavior.

## My Current Setup

In `Codex-Instructions`, the clean split should be:

- `AGENTS.md`: local instructions for maintaining this instruction repository.
- `SKILL.md`: reusable data science workflow, code style, plotting style, and collaboration style.
- `Sync-CodexInstructions.py`: helper script to copy `AGENTS.md` and `SKILL.md` into another project.
- `notes.md`: explanatory notes like this one.

When I sync into `Path To UTMB`, that project receives:

- `AGENTS.md`
- `SKILL.md`

That means `Path To UTMB` has a local copy of the current instruction setup.

## How To Sync Instructions To Another Project

From the `Codex-Instructions` folder:

```powershell
py .\Sync-CodexInstructions.py "C:\Users\Urh\Desktop\Urh\Github Repositories\Path To UTMB"
```

To sync another project, replace the path inside quotes.

The script copies:

- `AGENTS.md`
- `SKILL.md`

It overwrites older versions in the target project.

## Best Practice For My Use Case

Use this rule:

- Put repo-specific rules in `AGENTS.md`.
- Put reusable behavior in `SKILL.md`.
- Use `notes.md` for explanation and learning.
- Use the sync script when a project should receive the current instruction files.

Examples:

- "In this repo, source data is read-only" -> `AGENTS.md`
- "When plotting, use `ax[0,0]` and spaced kwargs like `s = 8`" -> `SKILL.md`
- "What is the difference between AGENTS and skills?" -> `notes.md`
- "Copy my current instruction setup into Path To UTMB" -> sync script

## What To Watch Out For

Too much in `AGENTS.md`:

- Makes every task in the repo carry long instruction baggage.
- Can become hard to maintain.
- Can mix local rules with reusable style preferences.

Too much in `SKILL.md`:

- Can make the skill too broad.
- Can make Codex use it when it is not relevant.
- Can make the actual task harder to focus on.

Too many installed community skills:

- Can make skill selection noisy.
- Can introduce conflicting instructions.
- Can add unreviewed behavior.

The best setup is boring in a good way:

- small local `AGENTS.md`
- focused reusable `SKILL.md`
- clear notes
- deliberate syncing

## Sources Checked

- Official Codex manual, fetched locally through the OpenAI docs skill on 2026-07-05.
- OpenAI Codex manual sections: Agent Skills, Custom instructions with `AGENTS.md`, Plugins.
- `skills.sh` homepage: https://www.skills.sh/
- `skills.sh` documentation: https://www.skills.sh/docs
- `skills.sh` CLI documentation: https://www.skills.sh/docs/cli
- `skills.sh` FAQ: https://www.skills.sh/docs/faq
- `skills.sh` customize docs: https://www.skills.sh/docs/customize
