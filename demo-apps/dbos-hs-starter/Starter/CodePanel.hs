{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TemplateHaskell #-}

-- | Compile-time embedding for the page's code panels.
--
-- The panels must show the code that actually runs. Each panel is a real
-- top-level definition extracted from its source file at compile time
-- ('addDependentFile' keeps the splice honest: editing the source forces the
-- panel to recompile), HTML-escaped and highlighted mechanically with the
-- same classes the CSS already styles. Nothing here is hand-written code:
-- the source of truth is the @.hs@ file beside this module.
module Starter.CodePanel
  ( panel,
    panelBlock,
    fragment,
    panelGap,
  )
where

import Data.Char (isAlpha, isAlphaNum, isDigit, isSpace)
import Data.List (isInfixOf, isPrefixOf)
import Language.Haskell.TH
import Language.Haskell.TH.Syntax (addDependentFile, lift)
import Prelude

-- | Embed one definition as panel HTML. @tags@ attach a CSS class to every
-- line containing the given substring (the page's JS walks the highlight
-- through these lines by class).
panel :: FilePath -> String -> [(String, String)] -> Q Exp
panel path name tags = do
  addDependentFile path
  source <- runIO (readFile path)
  let block = extractDefinition name (lines source)
  if null block
    then fail ("Starter.CodePanel: no definition named " <> name <> " in " <> path)
    else lift (renderPanel (tagLines tags block))

-- | A blank spacer line between two snippets rendered into one panel.
panelGap :: String
panelGap = "<span class=\"code-line\"> </span>"

-- | Embed one definition as panel HTML, wrapped in a span carrying @classes@
-- so the page can highlight the whole block as a unit (the workflow timeline's
-- step bodies use this, as the Rust starter's page does).
panelBlock :: FilePath -> String -> [String] -> [(String, String)] -> Q Exp
panelBlock path name classes tags = do
  addDependentFile path
  source <- runIO (readFile path)
  let block = extractDefinition name (lines source)
  if null block
    then fail ("Starter.CodePanel: no definition named " <> name <> " in " <> path)
    else lift ("<span class=\"" <> unwords classes <> "\">" <> renderPanel (tagLines tags block) <> "</span>")

-- | Embed a contiguous real-source fragment, delimited by anchors: one anchor
-- selects the first line containing it; several select from the first line
-- containing the first anchor through the first line containing the last
-- anchor at or after it. The fragment is dedented so it reads at column zero,
-- and the compile fails when an anchor matches nothing (the page can never
-- show code that moved).
fragment :: FilePath -> [String] -> [(String, String)] -> Q Exp
fragment path anchors tags = do
  addDependentFile path
  source <- runIO (readFile path)
  let block = extractFragment anchors (lines source)
  if null block
    then fail ("Starter.CodePanel: no fragment " <> show anchors <> " in " <> path)
    else lift (renderPanel (tagLines tags (dedent block)))

-- * Extraction

-- | The definition as it stands in the source: the signature line (when
-- present) and the body, up to the next top-level declaration or blank
-- separation. Trailing blank lines are trimmed.
extractDefinition :: String -> [String] -> [String]
extractDefinition name source =
  case dropWhile (not . startsDefinition) source of
    [] -> []
    (first : rest) ->
      if isSignature first
        then case dropWhile (all isSpace) rest of
          [] -> trimEnd [first]
          (headLine : more) -> trimEnd (first : headLine : takeWhile (not . startsTopLevel) more)
        else trimEnd (first : takeWhile (not . startsTopLevel) rest)
  where
    isSignature line = (name <> " ::") `isPrefixOf` line
    startsDefinition line =
      (name <> " ::") `isPrefixOf` line || (name <> " ") `isPrefixOf` line || (name <> " =") `isPrefixOf` line
    -- A column-0 line is the next declaration or section comment: the
    -- signature's own definition head is consumed above, before this.
    startsTopLevel line =
      case line of
        (c : _) -> not (isSpace c)
        [] -> False

-- | The contiguous source lines an anchor list points at. One anchor selects
-- the first line containing it; several select from the first line containing
-- the first anchor through the first line containing the last anchor at or
-- after it. The end anchor is bounded by its first match on purpose: an
-- "any closing paren" anchor must not run away to the end of the file.
extractFragment :: [String] -> [String] -> [String]
extractFragment [] _ = []
extractFragment anchors source =
  case (start, end) of
    (Just startLine, Just endLine) -> trimEnd (take (endLine - startLine + 1) (drop startLine source))
    _ -> []
  where
    indexed = zip [0 :: Int ..] source
    start = firstIndex (head anchors)
    end = case anchors of
      [_] -> start
      _ -> firstIndexAfter (head anchors) (last anchors)
    firstIndex needle = firstOf [i | (i, line) <- indexed, needle `isInfixOf` line]
    firstIndexAfter from to = do
      fromLine <- firstIndex from
      firstOf [i | (i, line) <- indexed, i >= fromLine, to `isInfixOf` line]
    firstOf (i : _) = Just i
    firstOf [] = Nothing

-- | Drop a block's common indentation, so a fragment reads at column zero.
dedent :: [String] -> [String]
dedent block = map (drop (commonIndent block)) block

commonIndent :: [String] -> Int
commonIndent block =
  case [length (takeWhile isSpace line) | line <- block, not (all isSpace line)] of
    [] -> 0
    indents -> minimum indents

-- | Trailing blank lines trimmed off an extracted block.
trimEnd :: [String] -> [String]
trimEnd = reverse . dropWhile (all isSpace) . reverse

-- * Highlighting

-- | The reserved words the panel colors; everything else stays default.
keywords :: [String]
keywords =
  [ "case",
    "class",
    "data",
    "default",
    "deriving",
    "do",
    "else",
    "foreign",
    "forall",
    "if",
    "import",
    "in",
    "infix",
    "infixl",
    "infixr",
    "instance",
    "let",
    "module",
    "newtype",
    "of",
    "qualified",
    "then",
    "type",
    "where"
  ]

operatorChars :: String
operatorChars = "=<>|&$+-*/^.@#?!:~"

-- | Mechanical highlighting: strings, comments, numbers, reserved words,
-- and operator runs get the CSS classes the page already styles; all other
-- text is escaped and left default.
highlight :: String -> String
highlight = go
  where
    go [] = []
    go ('"' : rest) =
      let (literal, rest') = spanString rest
       in "<span class=\"string\">&quot;" <> escape literal <> "&quot;</span>" <> go rest'
    go ('-' : '-' : rest) = "<span class=\"punct\">--" <> escape rest <> "</span>"
    go (c : rest)
      | isDigit c = let (digits, rest') = span isDigit rest in "<span class=\"num-lit\">" <> (c : digits) <> "</span>" <> go rest'
      | isAlpha c || c == '_' =
          let (word, rest') = span (\x -> isAlphaNum x || x == '_' || x == '\'') rest
              whole = c : word
           in if whole `elem` keywords
                then "<span class=\"keyword\">" <> whole <> "</span>" <> go rest'
                else whole <> go rest'
      | c `elem` operatorChars = let (ops, rest') = span (`elem` operatorChars) rest in "<span class=\"punct\">" <> (c : ops) <> "</span>" <> go rest'
      | otherwise = escape [c] <> go rest

    spanString [] = ([], [])
    spanString ('\\' : c : rest) = let (literal, rest') = spanString rest in (c : '\\' : literal, rest')
    spanString ('"' : rest) = ([], rest)
    spanString (c : rest) = let (literal, rest') = spanString rest in (c : literal, rest')

escape :: String -> String
escape = concatMap $ \case
  '&' -> "&amp;"
  '<' -> "&lt;"
  '>' -> "&gt;"
  '"' -> "&quot;"
  c -> [c]

-- * Rendering

tagLines :: [(String, String)] -> [String] -> [(String, String)]
tagLines tags block = [(classes line, line) | line <- block]
  where
    classes line = unwords [cls | (cls, needle) <- tags, needle `isInfixOf` line]

renderPanel :: [(String, String)] -> String
renderPanel = concatMap renderLine
  where
    renderLine (classes, line) =
      "<span class=\"code-line"
        <> (if null classes then "" else " " <> classes)
        <> "\">"
        <> highlight line
        <> "</span>"
