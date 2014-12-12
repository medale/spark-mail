# Markdown with Pandoc

    > sudo apt-get install pandoc

## Beamer Slides
    > pandoc -t beamer SparkMailPresentation.md -o SparkMailPresentation.pdf

Or just run:
    > ./beamer

## Reveal.js
* [Download latest version of reveal.js](https://github.com/hakimel/reveal.js/releases)
* Untar and move resulting directory to this directory as reveal.js


    > pandoc -t revealjs -s SparkMailPresentation.md -o index.html

Or just run:
    > ./reveal

## Resources
* [John McFarlane's Pandoc User Guide](http://johnmacfarlane.net/pandoc/README)
