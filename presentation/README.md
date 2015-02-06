# Markdown with Pandoc

On Ubuntu:
    > sudo apt-get install pandoc pandoc-citeproc
    > sudo apt-get install texlive-xetex

## Beamer Slides
    > pandoc -t beamer --latex-engine=xelatex --biblio ApacheSpark.bib SparkMailPresentation.md -o SparkMailPresentation.pdf

Or just run:
    > ./beamer

## Reveal.js
* Determine pandoc user directory
        pandoc --version
* [Download version 2.6.2 of reveal.js](https://github.com/hakimel/reveal.js/releases)
* Untar and move resulting directory to the pandoc user directory as reveal.js to make it available to all reveal builds


    > pandoc -t revealjs --biblio ApacheSpark.bib -s SparkMailPresentation.md -o index.html

Or just run:
    > ./reveal

## Resources
* [John McFarlane's Pandoc User Guide](http://johnmacfarlane.net/pandoc/README)
* [Stackoverflow on reveal.js location](http://stackoverflow.com/questions/21423952/self-contained-reveal-js-file-without-relative-reveal-js-folder-using-pandoc)
