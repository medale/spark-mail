# Markdown with Pandoc

On Ubuntu:
    > sudo apt-get install pandoc pandoc-citeproc
    > sudo apt-get install texlive-xetex

## Beamer Slides
    > pandoc -t beamer --latex-engine=xelatex --biblio ApacheSpark.bib SparkMailPresentation.md -o SparkMailPresentation.pdf

Or just run:
    > ./beamer

## Reveal.js
* [Download latest version of reveal.js](https://github.com/hakimel/reveal.js/releases)
* Untar and move resulting directory to this directory as reveal.js


    > pandoc -t revealjs --biblio ApacheSpark.bib -s SparkMailPresentation.md -o index.html

Or just run:
    > ./reveal

## Resources
* [John McFarlane's Pandoc User Guide](http://johnmacfarlane.net/pandoc/README)
